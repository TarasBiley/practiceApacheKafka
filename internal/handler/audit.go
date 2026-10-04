package handler

import (
	"audit-service/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// AuditHandler godoc
// @Summary Создать audit event
// @Description Сохраняет audit event и outbox в PostgreSQL одной транзакцией. Доставка в Kafka выполняется асинхронно.
// @Tags audit
// @Accept json
// @Produce json
// @Param request body model.AuditRequest true "Audit event"
// @Success 202 {object} model.AuditResponse
// @Failure 400 {string} string
// @Failure 500 {string} string
// @Router /api/audit [post]
func AuditHandler(
	db *pgxpool.Pool,
) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(
				w,
				"method not allowed",
				http.StatusMethodNotAllowed,
			)
			return
		}

		var req model.AuditRequest

		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			http.Error(
				w,
				"invalid json",
				http.StatusBadRequest,
			)
			return
		}

		if req.UserID == "" {
			http.Error(
				w,
				"user_id is required",
				http.StatusBadRequest,
			)
			return
		}

		if req.ResourceID == "" {
			http.Error(
				w,
				"resource_id is required",
				http.StatusBadRequest,
			)
			return
		}

		if req.Action != "login" &&
			req.Action != "view" &&
			req.Action != "purchase" {

			http.Error(
				w,
				"action must be login, view or purchase",
				http.StatusBadRequest,
			)
			return
		}

		if req.Meta == nil {
			req.Meta = map[string]any{}
		}

		eventID := uuid.New().String()
		timestamp := time.Now().UTC()

		tx, err := db.Begin(r.Context())
		if err != nil {
			http.Error(
				w,
				"failed to begin transaction",
				http.StatusInternalServerError,
			)
			return
		}

		defer tx.Rollback(context.Background())

		_, err = tx.Exec(
			r.Context(),
			`
			INSERT INTO audit_log (
				event_id,
				user_id,
				action,
				resource_id,
				meta,
				timestamp
			)
			VALUES ($1, $2, $3, $4, $5, $6)
			`,
			eventID,
			req.UserID,
			req.Action,
			req.ResourceID,
			req.Meta,
			timestamp,
		)

		if err != nil {
			http.Error(
				w,
				"failed to save audit event",
				http.StatusInternalServerError,
			)
			return
		}

		event := model.AuditEvent{
			EventID:    eventID,
			UserID:     req.UserID,
			Action:     req.Action,
			ResourceID: req.ResourceID,
			Meta:       req.Meta,
			Timestamp:  timestamp,
		}

		eventJSON, err := json.Marshal(event)
		if err != nil {
			http.Error(
				w,
				"failed to encode outbox event",
				http.StatusInternalServerError,
			)
			return
		}

		outboxID := uuid.New().String()

		_, err = tx.Exec(
			r.Context(),
			`
			INSERT INTO outbox (
				id,
				event_type,
				aggregate_id,
				payload
			)
			VALUES ($1, $2, $3, $4)
			`,
			outboxID,
			"audit_event",
			req.UserID,
			eventJSON,
		)

		if err != nil {
			http.Error(
				w,
				"failed to save outbox event",
				http.StatusInternalServerError,
			)
			return
		}

		err = tx.Commit(r.Context())
		if err != nil {
			http.Error(
				w,
				"failed to commit transaction",
				http.StatusInternalServerError,
			)
			return
		}

		response := model.AuditResponse{
			EventID:   eventID,
			Timestamp: timestamp,
		}

		w.Header().Set(
			"Content-Type",
			"application/json",
		)

		w.WriteHeader(http.StatusAccepted)

		if err := json.NewEncoder(w).Encode(response); err != nil {
			fmt.Println("failed to encode response:", err)
		}
	}
}

// GetAuditHandler godoc
// @Summary Получить историю audit событий
// @Description Возвращает события пользователя с фильтрацией и пагинацией
// @Tags audit
// @Produce json
// @Param user_id query string true "User ID"
// @Param action query string false "Action: login, view, purchase"
// @Param from query string false "Дата начала YYYY-MM-DD"
// @Param to query string false "Дата окончания YYYY-MM-DD"
// @Param page query int false "Номер страницы" default(1)
// @Param limit query int false "Количество элементов" default(50)
// @Success 200 {array} model.AuditEvent
// @Failure 400 {string} string
// @Failure 500 {string} string
// @Router /api/audit [get]
func GetAuditHandler(db *pgxpool.Pool) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(
				w,
				"method not allowed",
				http.StatusMethodNotAllowed,
			)
			return
		}

		userID := r.URL.Query().Get("user_id")
		action := r.URL.Query().Get("action")
		from := r.URL.Query().Get("from")
		to := r.URL.Query().Get("to")

		if userID == "" {
			http.Error(
				w,
				"user_id is required",
				http.StatusBadRequest,
			)
			return
		}

		if action != "" &&
			action != "login" &&
			action != "view" &&
			action != "purchase" {

			http.Error(
				w,
				"invalid action",
				http.StatusBadRequest,
			)
			return
		}

		page := 1
		limit := 50

		pageStr := r.URL.Query().Get("page")
		limitStr := r.URL.Query().Get("limit")

		if pageStr != "" {
			p, err := strconv.Atoi(pageStr)

			if err != nil || p < 1 {
				http.Error(
					w,
					"invalid page",
					http.StatusBadRequest,
				)
				return
			}

			page = p
		}

		if limitStr != "" {
			l, err := strconv.Atoi(limitStr)

			if err != nil || l < 1 || l > 100 {
				http.Error(
					w,
					"invalid limit",
					http.StatusBadRequest,
				)
				return
			}

			limit = l
		}

		var fromTime time.Time
		var toTime time.Time

		if from != "" {
			var err error

			fromTime, err = time.Parse(
				"2006-01-02",
				from,
			)

			if err != nil {
				http.Error(
					w,
					"invalid from date",
					http.StatusBadRequest,
				)
				return
			}
		}

		if to != "" {
			var err error

			toTime, err = time.Parse(
				"2006-01-02",
				to,
			)

			if err != nil {
				http.Error(
					w,
					"invalid to date",
					http.StatusBadRequest,
				)
				return
			}

			toTime = toTime.Add(24 * time.Hour)
		}

		offset := (page - 1) * limit

		query := `
			SELECT
				event_id,
				user_id,
				action,
				resource_id,
				meta,
				timestamp
			FROM audit_log
			WHERE user_id = $1
		`

		args := []any{userID}
		argPos := 2

		if action != "" {
			query += fmt.Sprintf(
				" AND action = $%d",
				argPos,
			)

			args = append(
				args,
				action,
			)

			argPos++
		}

		if from != "" {
			query += fmt.Sprintf(
				" AND timestamp >= $%d",
				argPos,
			)

			args = append(
				args,
				fromTime,
			)

			argPos++
		}

		if to != "" {
			query += fmt.Sprintf(
				" AND timestamp < $%d",
				argPos,
			)

			args = append(
				args,
				toTime,
			)

			argPos++
		}

		query += " ORDER BY timestamp DESC"

		query += fmt.Sprintf(
			" LIMIT $%d OFFSET $%d",
			argPos,
			argPos+1,
		)

		args = append(
			args,
			limit,
			offset,
		)

		rows, err := db.Query(
			context.Background(),
			query,
			args...,
		)

		if err != nil {
			http.Error(
				w,
				"failed to get audit history",
				http.StatusInternalServerError,
			)
			return
		}

		defer rows.Close()

		events := make(
			[]model.AuditEvent,
			0,
		)

		for rows.Next() {
			var event model.AuditEvent

			err := rows.Scan(
				&event.EventID,
				&event.UserID,
				&event.Action,
				&event.ResourceID,
				&event.Meta,
				&event.Timestamp,
			)

			if err != nil {
				http.Error(
					w,
					"failed to read audit event",
					http.StatusInternalServerError,
				)
				return
			}

			events = append(
				events,
				event,
			)
		}

		if err := rows.Err(); err != nil {
			http.Error(
				w,
				"failed to read audit history",
				http.StatusInternalServerError,
			)
			return
		}

		w.Header().Set(
			"Content-Type",
			"application/json",
		)

		err = json.NewEncoder(w).Encode(events)
		if err != nil {
			fmt.Println("failed to encode response:", err)
		}
	}
}
