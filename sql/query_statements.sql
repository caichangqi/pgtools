CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW query_statements AS (
  SELECT calls,
      (total_time / 1000) AS total_time,
      (total_time/calls / 1000) AS avg_time,
      regexp_replace(query, E'[\n\r\t ]+', ' ', 'ig') AS query
  FROM pg_stat_statements
  WHERE query <> ALL (ARRAY['SELECT $1;'::text, 'BEGIN'::text, 'COMMIT'::text, 'ROLLBACK'::text, 'DISCARD ALL;'::text]) AND query !~* 'vacuum' AND query !~* 'analyze' AND query !~* 'create index'
  ORDER BY avg_time desc
);
