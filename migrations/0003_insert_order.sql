-- created_at is the transaction start time, so rows written in one transaction
-- share it. seq breaks that tie in insertion order for ordered delivery.
-- Adding a serial column rewrites the table under an exclusive lock; existing
-- rows get seq values in physical order.
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS seq bigserial;

CREATE INDEX IF NOT EXISTS outbox_messages_partition_seq_idx
    ON outbox_messages (partition_key, created_at, seq)
    WHERE partition_key IS NOT NULL;
DROP INDEX IF EXISTS outbox_messages_partition_idx;
