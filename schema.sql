-- Desired state for a declarative migration planner, not an upgrade script.
-- Objects use the planner's target search_path.
CREATE TABLE outbox_messages (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    topic           text NOT NULL,
    partition_key   text,
    payload         bytea NOT NULL,
    headers         jsonb NOT NULL DEFAULT '{}'::jsonb,
    content_type    text,
    message_type    text,
    status          text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'published', 'dead', 'discarded')),
    attempts        int NOT NULL DEFAULT 0,
    max_attempts    int,
    locked_by       text,
    locked_until    timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz,
    lease_token     uuid,
    finished_at     timestamptz,
    -- @ratel.allow RTL-P02 reason="Preserve the sequence and default installed by the legacy migrations"
    seq             bigserial
);

CREATE INDEX outbox_messages_claim_idx
    ON outbox_messages (next_attempt_at, created_at)
    WHERE status IN ('pending', 'processing');

CREATE INDEX outbox_messages_cleanup_idx
    ON outbox_messages (published_at)
    WHERE status = 'published';

CREATE INDEX outbox_messages_finished_idx
    ON outbox_messages (finished_at)
    WHERE status IN ('dead', 'discarded');

CREATE INDEX outbox_messages_partition_seq_idx
    ON outbox_messages (partition_key, created_at, seq)
    WHERE partition_key IS NOT NULL;

CREATE FUNCTION outbox_messages_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('outbox_messages', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_messages_notify_trg
    AFTER INSERT ON outbox_messages
    FOR EACH STATEMENT EXECUTE FUNCTION outbox_messages_notify();

CREATE TABLE outbox_tombstones (
    id          uuid PRIMARY KEY,
    status      text NOT NULL CHECK (status IN ('dead', 'discarded')),
    finished_at timestamptz NOT NULL
);

CREATE INDEX outbox_tombstones_finished_idx
    ON outbox_tombstones (finished_at);
