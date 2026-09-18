-- Apply before starting v1.1 relays. Stop older relays before switching to the
-- new claim protocol: older binaries cannot fence their completion writes.
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS lease_token uuid;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS finished_at timestamptz;
ALTER TABLE outbox_messages DROP CONSTRAINT IF EXISTS outbox_messages_status_check;
ALTER TABLE outbox_messages ADD CONSTRAINT outbox_messages_status_check
    CHECK (status IN ('pending', 'processing', 'published', 'dead', 'discarded'));

-- Historical dead rows have no terminal timestamp. Start their retention
-- window at migration time rather than expiring them based on creation time.
UPDATE outbox_messages
SET finished_at = COALESCE(published_at, now())
WHERE status IN ('published', 'dead', 'discarded') AND finished_at IS NULL;

CREATE INDEX IF NOT EXISTS outbox_messages_finished_idx
    ON outbox_messages (finished_at)
    WHERE status IN ('dead', 'discarded');
