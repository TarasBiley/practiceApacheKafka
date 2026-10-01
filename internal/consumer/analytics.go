package consumer

import (
	"audit-service/internal/model"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func StartAnalyticsConsumer(
	db *pgxpool.Pool,
	kafkaReader *kafka.Reader,
	interval time.Duration,
	window time.Duration,
) func() {
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	workers.Add(2)
	var pendingMessages []kafka.Message
	var mu sync.Mutex

	ticker := time.NewTicker(interval)

	// Goroutine 1 — постоянно читает Kafka.
	go func() {
		defer workers.Done()
		for {
			msg, err := kafkaReader.FetchMessage(
				ctx,
			)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Println("consumer error:", err)
				return
			}

			var event model.AuditEvent

			err = json.Unmarshal(
				msg.Value,
				&event,
			)
			if err != nil {
				fmt.Println(
					"invalid kafka message:",
					err,
					"partition:",
					msg.Partition,
					"offset:",
					msg.Offset,
				)

				// Сообщение некорректное.
				// Считаем его обработанным, чтобы оно
				// не блокировало consumer постоянно.
				mu.Lock()
				pendingMessages = append(
					pendingMessages,
					msg,
				)
				mu.Unlock()

				continue
			}

			// Проверяем event_id.
			if _, err := uuid.Parse(event.EventID); err != nil {
				fmt.Println(
					"invalid event_id:",
					event.EventID,
					"partition:",
					msg.Partition,
					"offset:",
					msg.Offset,
				)

				mu.Lock()
				pendingMessages = append(
					pendingMessages,
					msg,
				)
				mu.Unlock()

				continue
			}

			// Проверяем action.
			if event.Action != "login" &&
				event.Action != "view" &&
				event.Action != "purchase" {

				fmt.Println(
					"invalid action:",
					event.Action,
					"partition:",
					msg.Partition,
					"offset:",
					msg.Offset,
				)

				mu.Lock()
				pendingMessages = append(
					pendingMessages,
					msg,
				)
				mu.Unlock()

				continue
			}

			// Проверяем timestamp.
			if event.Timestamp.IsZero() {
				fmt.Println(
					"invalid timestamp:",
					event.EventID,
					"partition:",
					msg.Partition,
					"offset:",
					msg.Offset,
				)

				mu.Lock()
				pendingMessages = append(
					pendingMessages,
					msg,
				)
				mu.Unlock()

				continue
			}

			// Сохраняем Kafka-событие.
			for {
				_, err = db.Exec(
					ctx,
					`
				INSERT INTO analytics_events (
					event_id,
					action,
					timestamp,
					kafka_partition,
					kafka_offset
				)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (event_id) DO NOTHING
				`,
					event.EventID,
					event.Action,
					event.Timestamp,
					msg.Partition,
					msg.Offset,
				)

				if err == nil {
					break
				}
				if ctx.Err() != nil {
					return
				}

				// Временная проблема БД → повторяем.
				if isRetryableDBError(err) {
					fmt.Println(
						"temporary database error, retrying:",
						err,
					)

					retryTimer := time.NewTimer(2 * time.Second)
					select {
					case <-ctx.Done():
						retryTimer.Stop()
						return
					case <-retryTimer.C:
					}
					continue
				}

				// Постоянная ошибка → consumer останавливаем.
				// Offset этого сообщения не подтверждается.
				fmt.Println(
					"permanent database error, stopping consumer:",
					err,
				)

				return
			}

			// Сообщение успешно обработано,
			// но offset пока ещё не commit.
			mu.Lock()

			pendingMessages = append(
				pendingMessages,
				msg,
			)

			mu.Unlock()

			fmt.Println(
				"key:", string(msg.Key),
				"user:", event.UserID,
				"action:", event.Action,
				"partition:", msg.Partition,
				"offset:", msg.Offset,
			)
		}
	}()
	// Goroutine 2 — периодически считает статистику.
	go func() {
		defer workers.Done()
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			now := time.Now().UTC()
			windowStart := now.Add(-window)

			// Берём snapshot сообщений,
			// offsets которых ещё не подтверждены.
			mu.Lock()

			messagesCopy := append(
				[]kafka.Message(nil),
				pendingMessages...,
			)

			mu.Unlock()

			// Статистику теперь считаем из PostgreSQL,
			// а не из памяти процесса.
			rows, err := db.Query(
				ctx,
				`
				SELECT
					action,
					COUNT(*)
				FROM analytics_events
				WHERE timestamp >= $1
				  AND timestamp < $2
				GROUP BY action
				`,
				windowStart,
				now,
			)

			if err != nil {
				fmt.Println(
					"failed to calculate stats:",
					err,
				)
				continue
			}

			stats := map[string]int{
				"login":    0,
				"view":     0,
				"purchase": 0,
			}

			for rows.Next() {
				var action string
				var count int

				err := rows.Scan(
					&action,
					&count,
				)

				if err != nil {
					fmt.Println(
						"failed to scan stats:",
						err,
					)
					continue
				}

				stats[action] = count
			}

			if err := rows.Err(); err != nil {
				fmt.Println(
					"failed to read stats:",
					err,
				)

				rows.Close()
				continue
			}

			rows.Close()

			// Теперь сохраняем snapshot статистики.
			tx, err := db.Begin(
				ctx,
			)

			if err != nil {
				fmt.Println(
					"failed to begin transaction:",
					err,
				)
				continue
			}

			saveSuccessful := true

			for action, count := range stats {
				if count == 0 {
					continue
				}

				_, err = tx.Exec(
					ctx,
					`
					INSERT INTO stats_cache (
						action,
						count,
						period_start,
						period_end
					)
					VALUES ($1, $2, $3, $4)
					`,
					action,
					count,
					windowStart,
					now,
				)

				if err != nil {
					fmt.Println(
						"failed to save stats:",
						err,
					)

					rollback(tx)

					saveSuccessful = false
					break
				}
			}

			if !saveSuccessful {
				continue
			}

			err = tx.Commit(
				ctx,
			)

			if err != nil {
				rollback(tx)
				fmt.Println(
					"failed to commit transaction:",
					err,
				)
				continue
			}

			// Только после успешной записи статистики передаём offsets
			// на коммит. При CommitInterval > 0 отправка выполняется асинхронно.
			if len(messagesCopy) > 0 {
				err = kafkaReader.CommitMessages(
					ctx,
					messagesCopy...,
				)

				if err != nil {
					fmt.Println(
						"failed to commit offsets:",
						err,
					)
					continue
				}
			}

			fmt.Println(
				"stats saved:",
				stats,
				"messages queued for offset commit:",
				len(messagesCopy),
			)

			// Удаляем только те сообщения, offsets которых передали
			// библиотеке для коммита и повторных попыток при ошибке.
			mu.Lock()

			if len(messagesCopy) <= len(pendingMessages) {
				pendingMessages =
					pendingMessages[len(messagesCopy):]
			}

			mu.Unlock()
		}
	}()

	return func() {
		cancel()
		workers.Wait()
	}
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func isRetryableDBError(err error) bool {
	var pgErr *pgconn.PgError

	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001": // serialization_failure
			return true

		case "40P01": // deadlock_detected
			return true

		case "53300": // too_many_connections
			return true

		case "57P01", // admin_shutdown
			"57P02", // crash_shutdown
			"57P03": // cannot_connect_now
			return true
		}

		// SQLSTATE 08xxx = connection errors.
		if len(pgErr.Code) >= 2 &&
			pgErr.Code[:2] == "08" {
			return true
		}

		return false
	}

	// Соединение с PostgreSQL внезапно оборвалось.
	if errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.EOF) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return false
}
