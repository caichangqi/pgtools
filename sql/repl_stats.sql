CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE FUNCTION pg_stat_repl()
RETURNS SETOF pg_catalog.pg_stat_replication
AS $$
  BEGIN 
    RETURN query(SELECT * FROM pg_catalog.pg_stat_replication);
  END
$$ LANGUAGE plpgsql security definer;

CREATE OR REPLACE VIEW repl_stats AS (
  SELECT pid, usesysid, usename, application_name, client_addr, client_hostname, client_port, backend_start, state, sent_location, write_location, flush_location, replay_location, sync_priority, sync_state,
      pg_size_pretty(pg_xlog_location_diff(sent_location, write_location)) AS network_delay,
      pg_size_pretty(pg_xlog_location_diff(write_location, flush_location)) AS slave_write,
      pg_size_pretty(pg_xlog_location_diff(flush_location, replay_location)) AS slave_replay,
      CASE
          WHEN pg_is_in_recovery() THEN pg_size_pretty(pg_xlog_location_diff(pg_last_xlog_replay_location(), replay_location))
          ELSE pg_size_pretty(pg_xlog_location_diff(pg_current_xlog_location(), replay_location))
      END AS total_lag
  FROM pg_stat_repl()
  ORDER BY client_addr
);
