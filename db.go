package salmonide

import (
	"context"
	"database/sql"
	"log"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

var logger = log.New(os.Stderr, "DB | ", log.LstdFlags)

type MigrationFn = func(ctx context.Context, tx *sql.Tx) error

func OpenDB(ctx context.Context, fileName string, migrations []MigrationFn) (db *sql.DB, cleanup func() error, err error) {
	logger.Println("opening database at", fileName)

	db, err = sql.Open("sqlite3", fileName+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, nil, err
	}
	cleanup = db.Close

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return db, cleanup, err
	}
	if err := applyMigrations(ctx, tx, migrations); err != nil {
		_ = tx.Rollback()
		return db, cleanup, err
	}
	return db, cleanup, tx.Commit()
}

func applyMigrations(ctx context.Context, tx *sql.Tx, migrations []MigrationFn) error {
	logger.Println("applying database migrations...")

	if _, err := tx.ExecContext(ctx, `
		create table if not exists _migration_status (
			id integer primary key check (id = 0),
			level integer
		)
	`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "select level from _migration_status")
	if err != nil {
		return err
	}

	var migrationLevel int
	if rows.Next() {
		var m *int
		if err := rows.Scan(&m); err != nil {
			return err
		}
		if m != nil {
			migrationLevel = *m
		}
	}

	for i, migration := range migrations[:len(migrations)-migrationLevel] {
		logger.Printf("applying database migration %d...", i)
		if err := migration(ctx, tx); err != nil {
			return err
		}
	}

	migrationLevel = len(migrations)
	result, err := tx.ExecContext(ctx, "update _migration_status set level = ?", migrationLevel)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		if _, err := tx.ExecContext(ctx, "insert into _migration_status (id, level) values (0, ?)", migrationLevel); err != nil {
			return err
		}
	}

	return nil
}
