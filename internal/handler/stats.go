package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ActionStat struct {
	Action string `json:"action"`
	Count  int    `json:"count"`
}

type DayStat struct {
	Day   time.Time `json:"day"`
	Count int       `json:"count"`
}

// StatsItem используется для Swagger-документации.
//
// При group_by=action:
//
//	{
//	  "action": "login",
//	  "count": 5
//	}
//
// При group_by=day:
//
//	{
//	  "day": "2026-10-01T00:00:00Z",
//	  "count": 10
//	}
type StatsItem struct {
	Action string    `json:"action,omitempty"`
	Day    time.Time `json:"day,omitempty"`
	Count  int       `json:"count"`
}

// GetStatsHandler godoc
// @Summary Получить статистику
// @Description Возвращает агрегированную статистику пользователя. При group_by=action ответ содержит action и count. При group_by=day ответ содержит day и count.
// @Tags stats
// @Produce json
// @Param user_id query string true "User ID"
// @Param group_by query string true "Группировка: action или day"
// @Success 200 {array} StatsItem
// @Failure 400 {string} string
// @Failure 500 {string} string
// @Router /api/stats [get]
func GetStatsHandler(db *pgxpool.Pool) http.HandlerFunc {

	// Альтернатива:
	// потоковую агрегацию статистики можно реализовать
	// с помощью Kafka Streams или ksqlDB.

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
		groupBy := r.URL.Query().Get("group_by")

		if userID == "" {
			http.Error(
				w,
				"user_id is required",
				http.StatusBadRequest,
			)
			return
		}

		if groupBy != "action" && groupBy != "day" {
			http.Error(
				w,
				"group_by must be action or day",
				http.StatusBadRequest,
			)
			return
		}

		if groupBy == "action" {
			rows, err := db.Query(
				context.Background(),
				`
				SELECT
					action,
					COUNT(*)
				FROM audit_log
				WHERE user_id = $1
				GROUP BY action
				ORDER BY action
				`,
				userID,
			)

			if err != nil {
				http.Error(
					w,
					"failed to get stats",
					http.StatusInternalServerError,
				)
				return
			}
			defer rows.Close()

			stats := make([]ActionStat, 0)

			for rows.Next() {
				var stat ActionStat

				err := rows.Scan(
					&stat.Action,
					&stat.Count,
				)
				if err != nil {
					http.Error(
						w,
						"failed to read stats",
						http.StatusInternalServerError,
					)
					return
				}

				stats = append(stats, stat)
			}

			if err := rows.Err(); err != nil {
				http.Error(
					w,
					"failed to read stats",
					http.StatusInternalServerError,
				)
				return
			}

			w.Header().Set(
				"Content-Type",
				"application/json",
			)

			json.NewEncoder(w).Encode(stats)
			return
		}

		if groupBy == "day" {
			rows, err := db.Query(
				context.Background(),
				`
				SELECT
					DATE(timestamp),
					COUNT(*)
				FROM audit_log
				WHERE user_id = $1
				GROUP BY DATE(timestamp)
				ORDER BY DATE(timestamp)
				`,
				userID,
			)

			if err != nil {
				http.Error(
					w,
					"failed to get stats",
					http.StatusInternalServerError,
				)
				return
			}
			defer rows.Close()

			stats := make([]DayStat, 0)

			for rows.Next() {
				var stat DayStat

				err := rows.Scan(
					&stat.Day,
					&stat.Count,
				)
				if err != nil {
					http.Error(
						w,
						"failed to read stats",
						http.StatusInternalServerError,
					)
					return
				}

				stats = append(stats, stat)
			}

			if err := rows.Err(); err != nil {
				http.Error(
					w,
					"failed to read stats",
					http.StatusInternalServerError,
				)
				return
			}

			w.Header().Set(
				"Content-Type",
				"application/json",
			)

			json.NewEncoder(w).Encode(stats)
			return
		}
	}
}
