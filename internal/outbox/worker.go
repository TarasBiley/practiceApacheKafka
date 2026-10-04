package outbox

import (
	"context"
	"fmt"
	"time"

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

				rows, err := db.Query(
					ctx,
					`
					SELECT
						id::text,
						aggregate_id,
						payload
					FROM outbox
					WHERE status = 'pending'
					ORDER BY created_at
					LIMIT 100
					`,
				)
				if err != nil {
					fmt.Println("outbox select error:", err)
					break
				}

				var (
					ids      []string
					messages []kafka.Message
				)

				for rows.Next() {
					var (
						id          string
						aggregateID string
						payload     []byte
					)

					if err := rows.Scan(
						&id,
						&aggregateID,
						&payload,
					); err != nil {
						fmt.Println("outbox scan error:", err)
						continue
					}

					ids = append(ids, id)

					messages = append(
						messages,
						kafka.Message{
							Key:   []byte(aggregateID),
							Value: payload,
						},
					)
				}

				if err := rows.Err(); err != nil {
					rows.Close()
					fmt.Println("outbox rows error:", err)
					break
				}

				rows.Close()

				if len(messages) == 0 {
					break
				}

				kafkaCtx, kafkaCancel := context.WithTimeout(
					ctx,
					5*time.Second,
				)

				err = writer.WriteMessages(
					kafkaCtx,
					messages...,
				)

				kafkaCancel()

				if err != nil {
					if ctx.Err() != nil {
						return
					}

					fmt.Println("outbox kafka error:", err)
					break
				}

				_, err = db.Exec(
					ctx,
					`
					UPDATE outbox
					SET
						status = 'sent',
						sent_at = NOW()
					WHERE id = ANY($1::uuid[])
					`,
					ids,
				)

				if err != nil {
					fmt.Println("outbox update error:", err)
					break
				}

				fmt.Println(
					"outbox batch sent:",
					len(messages),
				)
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}
