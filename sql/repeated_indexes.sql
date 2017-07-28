CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW repeated_indexes AS (
  SELECT key,
      indexrelid AS idx_oid,
      schemaname AS schema_name,
      tablename AS table_name,
      indexname AS idx_name,
      indisprimary,
      idx_scan, idx_tup_read, idx_tup_fetch,
      idx_size
  FROM (
      SELECT pi.indexrelid, pi.indisprimary, pis.schemaname, pis.tablename, pis.indexname, psui.idx_scan, psui.idx_tup_read, psui.idx_tup_fetch,
          pg_size_pretty(pg_relation_size(pi.indexrelid)) AS idx_size,
          pi.indrelid::text || pi.indclass::text || pi.indkey::text || COALESCE(pi.indexprs::text, '') || COALESCE(pi.indpred::text, '') AS key,
          count(*) OVER (PARTITION BY pi.indrelid::text || pi.indclass::text || pi.indkey::text || COALESCE(pi.indexprs::text, '') || COALESCE(pi.indpred::text, '')) AS n_rpt_idx
      FROM pg_index pi, pg_indexes pis, pg_stat_user_indexes psui
      WHERE pi.indexrelid::regclass::text = pis.indexname AND pi.indexrelid = psui.indexrelid AND pis.schemaname NOT IN ('pg_catalog', 'information_schema') 
  ) sub
  WHERE n_rpt_idx > 1
  ORDER BY idx_size DESC
);
