CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW query_activity AS (
  SELECT datid, datname, pid, usesysid, usename, application_name, client_addr, client_hostname, client_port, backend_start, xact_start, query_start, state_change, waiting, state,
      now() - query_start AS runtime,
      regexp_replace(query, E'[\n\r\t ]+', ' ', 'ig') AS query
  FROM pg_stat_activity 
  WHERE pid <> pg_backend_pid()
  ORDER BY runtime DESC
);
