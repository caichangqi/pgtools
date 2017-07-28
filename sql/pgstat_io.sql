CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW pgstat_io AS (
  SELECT pg_stat_user_tables.schemaname, pg_stat_user_tables.relname,
        ((((COALESCE(pg_stat_user_tables.seq_tup_read, (0)::bigint) + COALESCE(pg_stat_user_tables.idx_tup_fetch, (0)::bigint)) + COALESCE(pg_stat_user_tables.n_tup_ins, (0)::bigint)) + COALESCE(pg_stat_user_tables.n_tup_upd, (0)::bigint)) + COALESCE(pg_stat_user_tables.n_tup_del, (0)::bigint)) AS total_tuple,
        (COALESCE(pg_stat_user_tables.seq_tup_read, (0)::bigint) + COALESCE(pg_stat_user_tables.idx_tup_fetch, (0)::bigint)) AS total_select,
        COALESCE(pg_stat_user_tables.n_tup_ins, (0)::bigint) AS total_insert,
        COALESCE(pg_stat_user_tables.n_tup_upd, (0)::bigint) AS total_update,
        COALESCE(pg_stat_user_tables.n_tup_del, (0)::bigint) AS total_delete
  FROM pg_stat_user_tables
  ORDER BY total_tuple DESC
);
