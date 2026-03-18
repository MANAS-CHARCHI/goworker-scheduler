package internals

import (
	"database/sql"
	"fmt"
)

func EnsureTrigger(db *sql.DB, table string) error {
	// Create the trigger function
	_, err := db.Exec(`
		CREATE OR REPLACE FUNCTION notify_scheduler_func()
		RETURNS trigger AS $$
		BEGIN
			PERFORM pg_notify('` + PgChannel + `', NEW.id::text);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`)
	if err != nil {
		return fmt.Errorf("create trigger function: %w", err)
	}

	// Drop old trigger if exists then recreate (idempotent)
	_, err = db.Exec(fmt.Sprintf(`
		DROP TRIGGER IF EXISTS scheduler_notify_trigger ON %s;
		CREATE TRIGGER scheduler_notify_trigger
		AFTER INSERT OR UPDATE ON %s
		FOR EACH ROW EXECUTE FUNCTION notify_scheduler_func();
	`, table, table))
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}

	return nil
}
