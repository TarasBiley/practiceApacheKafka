package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func StartWorker(
	db *pgxpool.Pool,
	writer *kafka.Writer,
	interval time.Duration,
) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	ticker := time.NewTicker(interval)

	go func() {
		defer close(done)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			for {
				if ctx.Err() != nil {
					return
				}

				tx, err := db.Begin(ctx)
				if err != nil {
					fmt.Println(
						"outbox begin transaction error:",
						err,
					)
					break
				}

				var (
					id          string
					aggregateID string
					payload     []byte
				)

				err = tx.QueryRow(
					ctx,
					`
					SELECT
						id,
						aggregate_id,
						payload
					FROM outbox
					WHERE status = 'pending'
					ORDER BY created_at
					FOR UPDATE SKIP LOCKED
					LIMIT 1
					`,
				).Scan(
					&id,
					&aggregateID,
					&payload,
				)

				// Pending событий больше нет.
				if err == pgx.ErrNoRows {
					rollback(tx)
					break
				}

				if err != nil {
					rollback(tx)

					fmt.Println(
						"outbox select error:",
						err,
					)

					break
				}

				// Строка всё ещё заблокирована этой транзакцией.
				err = writer.WriteMessages(
					ctx,
					kafka.Message{
						Key:   []byte(aggregateID),
						Value: payload,
					},
				)

				if err != nil {
					// Rollback снимает блокировку.
					// status останется pending.
					rollback(tx)

					fmt.Println(
						"outbox kafka error:",
						err,
					)

					break
				}

				_, err = tx.Exec(
					ctx,
					`
					UPDATE outbox
					SET
						status = 'sent',
						sent_at = NOW()
					WHERE id = $1
					`,
					id,
				)

				if err != nil {
					rollback(tx)

					fmt.Println(
						"outbox update error:",
						err,
					)

					break
				}

				err = tx.Commit(ctx)
				if err != nil {
					rollback(tx)
					fmt.Println(
						"outbox commit error:",
						err,
					)

					break
				}

				fmt.Println(
					"outbox sent:",
					id,
				)
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
