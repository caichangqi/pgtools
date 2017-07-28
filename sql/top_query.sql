CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW top_query AS (
  SELECT datname, pid, usesysid, usename, application_name, client_addr, client_port, waiting, state,
      now() - query_start AS runtime,
      regexp_replace(query, E'[\n\r\t ]+', ' ', 'ig') AS query
  FROM pg_stat_activity 
  WHERE pid <> pg_backend_pid() AND state <> 'idle'
  ORDER BY runtime DESC
);
