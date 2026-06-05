-- Schema-unqualified on purpose: to install into a non-default schema, set
-- search_path on the migration connection. gen_random_uuid() is built into
-- PostgreSQL 13+; on older servers enable the pgcrypto extension.
CREATE TABLE IF NOT EXISTS outbox_messages (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    topic           text NOT NULL,
    partition_key   text,
    payload         bytea NOT NULL,
    headers         jsonb NOT NULL DEFAULT '{}'::jsonb,
    content_type    text,
    message_type    text,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'published', 'dead')),
    attempts        int NOT NULL DEFAULT 0,
    max_attempts    int,
    locked_by       text,
    locked_until    timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz
);

CREATE INDEX IF NOT EXISTS outbox_messages_claim_idx
    ON outbox_messages (next_attempt_at, created_at)
    WHERE status IN ('pending', 'processing');

CREATE INDEX IF NOT EXISTS outbox_messages_cleanup_idx
    ON outbox_messages (published_at)
    WHERE status = 'published';

CREATE INDEX IF NOT EXISTS outbox_messages_partition_idx
    ON outbox_messages (partition_key, created_at)
    WHERE partition_key IS NOT NULL;

CREATE OR REPLACE FUNCTION outbox_messages_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('outbox_messages', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS outbox_messages_notify_trg ON outbox_messages;
CREATE TRIGGER outbox_messages_notify_trg
    AFTER INSERT ON outbox_messages
    FOR EACH STATEMENT EXECUTE FUNCTION outbox_messages_notify();
