package coordinator

import (
	"context"
	"database/sql"

	"tbx.at/salmonide"
)

var DBMigrations []salmonide.MigrationFn = []salmonide.MigrationFn{
	func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			create table jobs (
				id integer primary key,

				exit_code integer,
				image text,
				shell_script text,
				status integer
			)
		`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			create table output_chunks (
				id integer primary key,
				
				content text,
				job_id integer,
				stream integer,
				timestamp datetime,

				foreign key (job_id) references jobs (id)
			)
		`); err != nil {
			return err
		}
		return nil
	},
}
