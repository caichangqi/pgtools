CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW streaming_timedelay AS (
SELECT
    CASE
        WHEN (pg_last_xlog_receive_location() = pg_last_xlog_replay_location()) THEN (0)::double precision
        ELSE date_part('epoch'::text, (now() - pg_last_xact_replay_timestamp()))
    END AS time_delay
);
