-- Compact outcome of every dead or discarded row the cleaner removed: no
-- payload, only what Published/WaitPublished need to stay exact. Kept for
-- WithTombstoneRetention, then removed by the same cleaner.
CREATE TABLE IF NOT EXISTS outbox_tombstones (
    id          uuid PRIMARY KEY,
    status      text NOT NULL CHECK (status IN ('dead', 'discarded')),
    finished_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS outbox_tombstones_finished_idx
    ON outbox_tombstones (finished_at);
