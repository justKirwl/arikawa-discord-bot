package repositories_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
)

func TestDatabaseConnection(t *testing.T) {
	_ = godotenv.Load()
	dbUrl := os.Getenv("PSQL_URL")

	t.Run("database ping test", func(t *testing.T) {
		if dbUrl == "" {
			t.Error("PSQL_URL is empty or don't exists")
		}

		db, err := sql.Open("pgx", dbUrl)
		if err != nil {
			t.Errorf("failed opening connection to postgres: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			t.Errorf("failed pinging database: %v", err)
		}
	})
}
