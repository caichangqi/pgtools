CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW relation_size AS (
  SELECT c.relkind,
      n.nspname AS schema_name,
      c.relname as relation_name,
      CASE WHEN c.relkind = 'i' THEN 'index'
        WHEN c.relkind = 'r' THEN 'ordinary table'
        WHEN c.relkind = 'S' THEN 'sequence'
        WHEN c.relkind = 'v' THEN 'view'
        WHEN c.relkind = 'm' THEN 'materialized view'
        WHEN c.relkind = 'c' THEN 'composite type'
        WHEN c.relkind = 't' THEN 'TOAST table'
        WHEN c.relkind = 'f' THEN 'foreign table'
      END AS relation_type,
      CASE
          WHEN c.relkind = 'r' THEN pg_size_pretty(pg_table_size(c.oid))
          ELSE pg_size_pretty(pg_total_relation_size(c.oid))
      END AS relation_size,
      pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
  FROM
      pg_class c
  LEFT JOIN
      pg_namespace n
  ON (n.oid = c.relnamespace)
  WHERE
      n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname !~ '^pg_toast'
  ORDER BY pg_total_relation_size(c.oid) DESC
);
