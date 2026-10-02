package handler

import (
	"audit-service/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// RebuildStatsHandler godoc
// @Summary Пересчитать статистику
// @Description Перечитывает историю Kafka за указанный период и пересчитывает статистику
// @Tags admin
// @Produce json
// @Param from query string true "Дата начала YYYY-MM-DD"
// @Param to query string true "Дата окончания YYYY-MM-DD"
// @Success 200 {object} map[string]int
// @Failure 400 {string} string
// @Failure 500 {string} string
// @Router /api/admin/rebuild-stats [post]
func RebuildStatsHandler(
	db *pgxpool.Pool,
	broker string,
	topic string,
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

		from := r.URL.Query().Get("from")
		to := r.URL.Query().Get("to")

		if from == "" || to == "" {
			http.Error(
				w,
				"from and to are required",
				http.StatusBadRequest,
			)
			return
		}

		fromTime, err := time.Parse(
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

		toTime, err := time.Parse(
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

		partitions, err := getPartitions(
			broker,
			topic,
		)
		if err != nil {
			http.Error(
				w,
				"failed to get kafka partitions",
				http.StatusInternalServerError,
			)
			return
		}

		fmt.Println(
			"kafka partitions:",
			len(partitions),
		)

		for _, partition := range partitions {
			firstOffset, lastOffset, err := getPartitionOffsets(
				broker,
				topic,
				partition.ID,
			)
			if err != nil {
				http.Error(
					w,
					"failed to get partition offsets",
					http.StatusInternalServerError,
				)
				return
			}

			fmt.Println(
				"partition:",
				partition.ID,
				"first offset:",
				firstOffset,
				"last offset:",
				lastOffset,
			)
		}

		// Делаем дату "to" включительно.
		//
		// Например:
		//
		// to=2026-10-02
		//
		// превращается в:
		//
		// timestamp < 2026-10-03 00:00
		toTime = toTime.Add(24 * time.Hour)

		stats := map[string]int{
			"login":    0,
			"view":     0,
			"purchase": 0,
		}

		seenEvents := make(map[string]bool)

		for _, partition := range partitions {
			firstOffset, lastOffset, err := getPartitionOffsets(
				broker,
				topic,
				partition.ID,
			)
			if err != nil {
				http.Error(
					w,
					"failed to get partition offsets",
					http.StatusInternalServerError,
				)
				return
			}

			fmt.Println(
				"reading partition:",
				partition.ID,
				"from:",
				firstOffset,
				"to:",
				lastOffset,
			)

			// Если partition пустая.
			if firstOffset >= lastOffset {
				continue
			}

			reader := kafka.NewReader(
				kafka.ReaderConfig{
					Brokers: []string{broker},
					Topic:   topic,

					// Читаем конкретную partition.
					Partition: partition.ID,
				},
			)

			err = reader.SetOffset(firstOffset)
			if err != nil {
				reader.Close()

				http.Error(
					w,
					"failed to set kafka offset",
					http.StatusInternalServerError,
				)
				return
			}

			for {
				ctx, cancel := context.WithTimeout(
					r.Context(),
					10*time.Second,
				)

				msg, err := reader.ReadMessage(ctx)

				cancel()

				if err != nil {
					reader.Close()

					http.Error(
						w,
						"failed to read kafka history",
						http.StatusInternalServerError,
					)
					return
				}

				// lastOffset — граница, которая НЕ включается.
				if msg.Offset >= lastOffset {
					break
				}

				var event model.AuditEvent

				err = json.Unmarshal(
					msg.Value,
					&event,
				)
				if err != nil {
					fmt.Println(
						"failed to decode replay message:",
						err,
					)

					// Переходим к следующему сообщению,
					// но сначала проверяем,
					// дошли ли до конца snapshot.
					if msg.Offset+1 >= lastOffset {
						break
					}

					continue
				}

				fmt.Println(
					"replay event:",
					"key:", string(msg.Key),
					"partition:", partition.ID,
					"offset:", msg.Offset,
					"id:", event.EventID,
					"action:", event.Action,
					"timestamp:", event.Timestamp,
				)

				// Событие попадает в нужный период.
				if !event.Timestamp.Before(fromTime) &&
					event.Timestamp.Before(toTime) {

					if event.EventID != "" {
						if seenEvents[event.EventID] {
							fmt.Println(
								"duplicate skipped:",
								event.EventID,
							)
						} else {
							seenEvents[event.EventID] = true
							stats[event.Action]++
						}
					} else {
						stats[event.Action]++
					}
				}

				// Например lastOffset = 2.
				// Последнее нужное сообщение имеет offset = 1.
				if msg.Offset+1 >= lastOffset {
					break
				}
			}

			err = reader.Close()
			if err != nil {
				fmt.Println(
					"failed to close rebuild reader:",
					err,
				)
			}
		}
		fmt.Println(
			"replay finished, stats:",
			stats,
		)

		// Сохраняем весь rebuild одной транзакцией.
		tx, err := db.Begin(
			context.Background(),
		)
		if err != nil {
			http.Error(
				w,
				"failed to begin transaction",
				http.StatusInternalServerError,
			)
			return
		}

		// Если rebuild этого периода уже запускался,
		// удаляем старые результаты.
		_, err = tx.Exec(
			context.Background(),
			`
			DELETE FROM stats_cache
			WHERE period_start = $1
			  AND period_end = $2
			`,
			fromTime,
			toTime,
		)

		if err != nil {
			_ = tx.Rollback(
				context.Background(),
			)

			http.Error(
				w,
				"failed to clear old stats",
				http.StatusInternalServerError,
			)
			return
		}

		// Записываем пересчитанную статистику.
		for action, count := range stats {
			if count == 0 {
				continue
			}

			_, err = tx.Exec(
				context.Background(),
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
				fromTime,
				toTime,
			)

			if err != nil {
				_ = tx.Rollback(
					context.Background(),
				)

				http.Error(
					w,
					"failed to rebuild stats",
					http.StatusInternalServerError,
				)
				return
			}
		}

		err = tx.Commit(
			context.Background(),
		)
		if err != nil {
			http.Error(
				w,
				"failed to commit rebuild",
				http.StatusInternalServerError,
			)
			return
		}

		fmt.Println(
			"rebuild from:",
			fromTime,
			"to:",
			toTime,
		)

		w.Header().Set(
			"Content-Type",
			"application/json",
		)

		w.WriteHeader(
			http.StatusOK,
		)

		err = json.NewEncoder(w).Encode(
			stats,
		)
		if err != nil {
			fmt.Println(
				"failed to encode response:",
				err,
			)
		}
	}
}

func getPartitionOffsets(
	broker string,
	topic string,
	partitionID int,
) (int64, int64, error) {

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	conn, err := kafka.DialLeader(
		ctx,
		"tcp",
		broker,
		topic,
		partitionID,
	)
	if err != nil {
		return 0, 0, err
	}

	defer conn.Close()

	// Чтобы ReadOffsets не мог зависнуть бесконечно.
	err = conn.SetDeadline(
		time.Now().Add(5 * time.Second),
	)
	if err != nil {
		return 0, 0, err
	}

	firstOffset, lastOffset, err := conn.ReadOffsets()
	if err != nil {
		return 0, 0, err
	}

	return firstOffset, lastOffset, nil
}
func getPartitions(
	broker string,
	topic string,
) ([]kafka.Partition, error) {

	conn, err := kafka.Dial(
		"tcp",
		broker,
	)
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, err
	}

	return partitions, nil
}
