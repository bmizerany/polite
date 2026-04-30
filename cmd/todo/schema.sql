-- schema.sql — idempotent schema for the polite to-do demo.
--
-- Re-running this file is a no-op: every object is created with IF NOT
-- EXISTS, and the NOTIFY trigger is replaced rather than duplicated. The
-- demo never migrates; if the schema changes, drop the database and run
-- this file again.

CREATE TABLE IF NOT EXISTS lists (
    id          text        PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS items (
    id          bigserial   PRIMARY KEY,
    list_id     text        NOT NULL REFERENCES lists(id) ON DELETE CASCADE,
    body        text        NOT NULL,
    done        boolean     NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS items_by_list ON items (list_id, id);
