package dbv2

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func NewPostgresDB(ctx context.Context, url string) error {
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		return err
	}
	_ = conn
	return nil
}
