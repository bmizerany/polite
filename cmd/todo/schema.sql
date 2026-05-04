-- schema.sql: idempotent schema for the polite to-do demo.
--
-- Statement separator: a line whose only non-whitespace content is
-- ";". A ";" anywhere else on a line is rejected by the loader, so
-- $$ ... $$ bodies and inline comments need no special parsing.
--
-- A single AFTER INSERT OR UPDATE trigger broadcasts the affected
-- list_id on the todo_changes channel, so the Go side runs plain
-- INSERT and UPDATE statements with no inline pg_notify call.
-- Subscribers re-fetch the list and re-render in place, which keeps
-- the payload fixed-size and the SSE handler stateless.

CREATE TABLE IF NOT EXISTS items (
    id         bigserial   PRIMARY KEY,
    list_id    text        NOT NULL
        CONSTRAINT items_list_id_format
        CHECK (list_id ~ '^[A-Za-z0-9_-]{1,64}$'),
    body       text        NOT NULL,
    done       boolean     NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
)
;

CREATE INDEX IF NOT EXISTS items_by_list ON items (list_id, id)
;

CREATE OR REPLACE FUNCTION items_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(
        'todo_changes',
        json_build_object('list_id', NEW.list_id)::text
    );
    RETURN NEW;
END;
$$
;

CREATE OR REPLACE TRIGGER items_notify_t
    AFTER INSERT OR UPDATE ON items
    FOR EACH ROW EXECUTE FUNCTION items_notify()
;
