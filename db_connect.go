package db_connect

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"arikawa-discord/ent"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
)

func main() {
	dsn := os.Getenv("PSQL_URL")
	if dsn == "" {
		log.Fatal("PSQL_URL environment variable is not set")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatalf("failed opening connection to postgres: %v", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	drv := entsql.OpenDB(dialect.Postgres, db)
	client := ent.NewClient(ent.Driver(drv))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("failed pinging database: %v", err)
	}
	if err := client.Schema.Create(ctx); err != nil {
		log.Fatalf("error in migration: %v", err)
	}

	log.Println("Successfully connected to Neon PostgreSQL!")
}
