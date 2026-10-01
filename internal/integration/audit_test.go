package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"audit-service/internal/consumer"
	"audit-service/internal/handler"
	"audit-service/internal/kafkaclient"
	"audit-service/internal/model"
	"audit-service/internal/outbox"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestAuditAPIAnalyticsReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	postgres, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:17",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "audit",
				"POSTGRES_PASSWORD": "audit",
				"POSTGRES_DB":       "audit",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	auditContainerCleanup(t, postgres)
	host, err := postgres.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := postgres.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatal(err)
	}
	dsn := "postgres://audit:audit@" + net.JoinHostPort(host, port.Port()) + "/audit?sslmode=disable&timezone=UTC"
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	migration, err := os.ReadFile("../../migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}

	kafkaContainer, err := kafkacontainer.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		t.Fatal(err)
	}
	auditContainerCleanup(t, kafkaContainer)
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const topic = "user-actions"
	conn, err := kafkago.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	err = conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 3, ReplicationFactor: 1})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Before the outbox worker runs, durable storage must not produce HTTP 201.
	shortServer := httptest.NewServer(handler.AuditHandler(db, 200*time.Millisecond))
	t.Cleanup(shortServer.Close)
	primaryID := "primary-user"
	pendingRequest := model.AuditRequest{UserID: primaryID, Action: "login", ResourceID: "session"}
	var pending model.AuditResponse
	auditRequestJSON(t, ctx, shortServer.Client(), http.MethodPost, shortServer.URL, pendingRequest,
		http.StatusServiceUnavailable, &pending)
	if _, err := uuid.Parse(pending.EventID); err != nil || pending.Timestamp.IsZero() {
		t.Fatalf("503 must identify the durably saved event: %+v, UUID error: %v", pending, err)
	}
	if status := auditOutboxStatus(t, ctx, db, pending.EventID); status != "pending" {
		t.Fatalf("outbox status before starting worker: got %q, want pending", status)
	}
	var saved int
	if err := db.QueryRow(ctx, "SELECT COUNT(*) FROM audit_log WHERE event_id = $1", pending.EventID).Scan(&saved); err != nil || saved != 1 {
		t.Fatalf("timed-out event must remain in audit_log: count=%d, err=%v", saved, err)
	}
	shortServer.Close()

	writer := kafkaclient.NewWriter(brokers[0], topic)
	t.Cleanup(func() { _ = writer.Close() })
	stopOutbox := outbox.StartWorker(db, writer, 100*time.Millisecond)
	t.Cleanup(stopOutbox)
	auditEventually(t, ctx, "pending event is delivered after the worker starts", func(ctx context.Context) (bool, error) {
		var status string
		err := db.QueryRow(ctx, "SELECT status FROM outbox WHERE payload->>'event_id' = $1", pending.EventID).Scan(&status)
		return status == "sent", err
	})

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/audit", handler.AuditHandler(db, 15*time.Second))
	mux.HandleFunc("GET /api/audit", handler.GetAuditHandler(db))
	mux.HandleFunc("GET /api/stats", handler.GetStatsHandler(db))
	mux.HandleFunc("POST /api/admin/rebuild-stats", handler.RebuildStatsHandler(db, brokers[0], topic))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 40 * time.Second

	requests := []model.AuditRequest{
		pendingRequest,
		{UserID: primaryID, Action: "view", ResourceID: "product", Meta: map[string]any{"source": "catalog"}},
		{UserID: "another-user", Action: "purchase", ResourceID: "order"},
	}
	responses := []model.AuditResponse{pending}
	for _, request := range requests[1:] {
		var response model.AuditResponse
		auditRequestJSON(t, ctx, client, http.MethodPost, server.URL+"/api/audit", request, http.StatusCreated, &response)
		if _, err := uuid.Parse(response.EventID); err != nil || response.Timestamp.IsZero() {
			t.Fatalf("invalid success response: %+v, UUID error: %v", response, err)
		}
		if status := auditOutboxStatus(t, ctx, db, response.EventID); status != "sent" {
			t.Fatalf("HTTP 201 before Kafka delivery: outbox status=%q", status)
		}
		responses = append(responses, response)
	}

	// An independent consumer verifies the acknowledged events exist in Kafka.
	reader := kafkaclient.NewReader(brokers[0], topic, "api-verification", 5*time.Second, 1024*1024)
	t.Cleanup(func() { _ = reader.Close() })
	expected := make(map[string]int)
	for i, response := range responses {
		expected[response.EventID] = i
	}
	messages := make(map[string]kafkago.Message)
	events := make(map[string]model.AuditEvent)
	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()
	for len(events) < len(expected) {
		message, err := reader.FetchMessage(readCtx)
		if err != nil {
			t.Fatal(err)
		}
		var event model.AuditEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			t.Fatal(err)
		}
		index, ok := expected[event.EventID]
		if !ok {
			t.Fatalf("unexpected event in isolated Kafka topic: %+v", event)
		}
		request := requests[index]
		if string(message.Key) != request.UserID || event.UserID != request.UserID ||
			event.Action != request.Action || event.ResourceID != request.ResourceID ||
			!event.Timestamp.Equal(responses[index].Timestamp) {
			t.Fatalf("Kafka payload/key do not match the acknowledged request: key=%q, event=%+v", message.Key, event)
		}
		if event.Meta == nil || (request.Meta != nil && !reflect.DeepEqual(event.Meta, request.Meta)) {
			t.Fatalf("Kafka metadata: got %v, want %v (or an empty object when omitted)", event.Meta, request.Meta)
		}
		messages[event.EventID] = message
		events[event.EventID] = event
	}
	first, second := messages[pending.EventID], messages[responses[1].EventID]
	if first.Partition != second.Partition || first.Offset >= second.Offset {
		t.Fatalf("events of one user must retain partition/order: first=%d/%d, second=%d/%d",
			first.Partition, first.Offset, second.Partition, second.Offset)
	}
	reader.Close()

	var history []model.AuditEvent
	auditRequestJSON(t, ctx, client, http.MethodGet, server.URL+"/api/audit?user_id="+primaryID+"&page=1&limit=1", nil, http.StatusOK, &history)
	if len(history) != 1 || history[0].EventID != responses[1].EventID {
		t.Fatalf("history page 1: %+v", history)
	}
	auditRequestJSON(t, ctx, client, http.MethodGet, server.URL+"/api/audit?user_id="+primaryID+"&page=2&limit=1", nil, http.StatusOK, &history)
	if len(history) != 1 || history[0].EventID != pending.EventID {
		t.Fatalf("history page 2: %+v", history)
	}
	replayDay := responses[1].Timestamp.UTC().Truncate(24 * time.Hour)
	day := replayDay.Format("2006-01-02")
	query := url.Values{"user_id": {primaryID}, "action": {"view"}, "from": {day}, "to": {day}}
	auditRequestJSON(t, ctx, client, http.MethodGet, server.URL+"/api/audit?"+query.Encode(), nil, http.StatusOK, &history)
	if len(history) != 1 || history[0].EventID != responses[1].EventID || history[0].Meta["source"] != "catalog" {
		t.Fatalf("history with action/date filters: %+v", history)
	}
	query.Set("from", replayDay.AddDate(0, 0, 1).Format("2006-01-02"))
	query.Set("to", query.Get("from"))
	auditRequestJSON(t, ctx, client, http.MethodGet, server.URL+"/api/audit?"+query.Encode(), nil, http.StatusOK, &history)
	if len(history) != 0 {
		t.Fatalf("history date filter included events outside the requested day: %+v", history)
	}

	var actionStats []handler.ActionStat
	auditRequestJSON(t, ctx, client, http.MethodGet, server.URL+"/api/stats?user_id="+primaryID+"&group_by=action", nil, http.StatusOK, &actionStats)
	if want := []handler.ActionStat{{Action: "login", Count: 1}, {Action: "view", Count: 1}}; !reflect.DeepEqual(actionStats, want) {
		t.Fatalf("action stats: got %+v, want %+v", actionStats, want)
	}
	var dayStats []handler.DayStat
	auditRequestJSON(t, ctx, client, http.MethodGet, server.URL+"/api/stats?user_id="+primaryID+"&group_by=day", nil, http.StatusOK, &dayStats)
	wantDays := make(map[string]int)
	for _, response := range responses[:2] {
		wantDays[response.Timestamp.UTC().Format("2006-01-02")]++
	}
	gotDays := make(map[string]int)
	for _, stat := range dayStats {
		gotDays[stat.Day.Format("2006-01-02")] = stat.Count
	}
	if !reflect.DeepEqual(gotDays, wantDays) {
		t.Fatalf("day stats: got %v, want %v", gotDays, wantDays)
	}

	// Replay must deduplicate a retry and filter by the event's timestamp.
	oldEvent := model.AuditEvent{EventID: uuid.NewString(), UserID: "old-user", Action: "login",
		ResourceID: "old-session", Meta: map[string]any{}, Timestamp: replayDay.Add(-48 * time.Hour)}
	oldPayload, err := json.Marshal(oldEvent)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteMessages(ctx,
		kafkago.Message{Key: second.Key, Value: second.Value},
		kafkago.Message{Key: []byte(oldEvent.UserID), Value: oldPayload},
	); err != nil {
		t.Fatal(err)
	}

	analyticsReader := kafkaclient.NewReader(brokers[0], topic, "analytics-group", 5*time.Second, 1024*1024)
	t.Cleanup(func() { _ = analyticsReader.Close() })
	stopAnalytics := consumer.StartAnalyticsConsumer(db, analyticsReader, 200*time.Millisecond, time.Hour)
	t.Cleanup(stopAnalytics)
	auditEventually(t, ctx, "analytics saves all unique Kafka events", func(ctx context.Context) (bool, error) {
		var count int
		err := db.QueryRow(ctx, "SELECT COUNT(*) FROM analytics_events").Scan(&count)
		return count == 4, err
	})
	wantHourly := map[string]int{"login": 1, "view": 1, "purchase": 1}
	auditEventually(t, ctx, "hourly statistics exclude old events and duplicate deliveries", func(ctx context.Context) (bool, error) {
		stats, err := auditReadStats(ctx, db, `SELECT action, count FROM stats_cache
			WHERE period_end = (SELECT MAX(period_end) FROM stats_cache)`)
		return reflect.DeepEqual(stats, wantHourly), err
	})
	stopAnalytics()
	analyticsReader.Close()

	wantReplay := map[string]int{"login": 0, "view": 0, "purchase": 0}
	for _, event := range events {
		if !event.Timestamp.Before(replayDay) && event.Timestamp.Before(replayDay.Add(24*time.Hour)) {
			wantReplay[event.Action]++
		}
	}
	for attempt := 1; attempt <= 2; attempt++ {
		var replay map[string]int
		auditRequestJSON(t, ctx, client, http.MethodPost, server.URL+"/api/admin/rebuild-stats?from="+day+"&to="+day,
			nil, http.StatusOK, &replay)
		if !reflect.DeepEqual(replay, wantReplay) {
			t.Fatalf("replay %d: got %v, want %v", attempt, replay, wantReplay)
		}
		stored, err := auditReadStats(ctx, db, `SELECT action, count FROM stats_cache WHERE period_start = $1 AND period_end = $2`,
			replayDay, replayDay.Add(24*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		wantStored := make(map[string]int)
		for action, count := range wantReplay {
			if count > 0 {
				wantStored[action] = count
			}
		}
		if !reflect.DeepEqual(stored, wantStored) {
			t.Fatalf("stored replay %d: got %v, want %v", attempt, stored, wantStored)
		}
	}
}

func auditContainerCleanup(t *testing.T, container testcontainers.Container) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := container.Terminate(ctx); err != nil {
			t.Errorf("terminate test container: %v", err)
		}
	})
}

func auditRequestJSON(t *testing.T, ctx context.Context, client *http.Client, method, address string, payload any, status int, result any) {
	t.Helper()
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, address, body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("%s %s: got HTTP %d, want %d, body=%s", method, address, response.StatusCode, status, data)
	}
	if result != nil {
		if err := json.Unmarshal(data, result); err != nil {
			t.Fatalf("decode %s %s: %v; body=%s", method, address, err, data)
		}
	}
}

func auditOutboxStatus(t *testing.T, ctx context.Context, db *pgxpool.Pool, eventID string) string {
	t.Helper()
	var status string
	if err := db.QueryRow(ctx, "SELECT status FROM outbox WHERE payload->>'event_id' = $1", eventID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func auditEventually(t *testing.T, parent context.Context, description string, check func(context.Context) (bool, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		ok, err := check(ctx)
		if ok && err == nil {
			return
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s (last error: %v)", description, lastErr)
		case <-ticker.C:
		}
	}
}

func auditReadStats(ctx context.Context, db *pgxpool.Pool, query string, args ...any) (map[string]int, error) {
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stats := make(map[string]int)
	for rows.Next() {
		var action string
		var count int
		if err := rows.Scan(&action, &count); err != nil {
			return nil, err
		}
		if _, duplicate := stats[action]; duplicate {
			return nil, fmt.Errorf("duplicate aggregate row for action %q", action)
		}
		stats[action] = count
	}
	return stats, rows.Err()
}
