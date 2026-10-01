package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WaitSent waits for the worker to persist Kafka's acknowledgement.
// It does not lock the row: the worker must be able to deliver it concurrently.
func WaitSent(ctx context.Context, db *pgxpool.Pool, id string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		var status string
		if err := db.QueryRow(ctx, `SELECT status FROM outbox WHERE id = $1`, id).Scan(&status); err != nil {
			return fmt.Errorf("read outbox delivery status: %w", err)
		}
		if status == "sent" {
			return nil
		}
		if status != "pending" {
			return fmt.Errorf("outbox delivery status: %s", status)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
