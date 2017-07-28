CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW buffer_stats AS (
SELECT c.relname,
    pg_size_pretty(count(*) * current_setting('block_size')::bigint) AS buffered,
    round(100.0 * count(*) / (SELECT setting FROM pg_settings WHERE name='shared_buffers')::integer,1) AS buffers_percent,
    round(100.0 * count(*) * current_setting('block_size')::bigint / pg_table_size(c.oid),1) AS percent_of_relation
FROM
    pg_class c
INNER JOIN
    pg_buffercache b
ON b.relfilenode = c.relfilenode
INNER JOIN
    pg_database d
ON (b.reldatabase = d.oid AND d.datname = current_database())
GROUP BY c.oid,c.relname
ORDER BY 3 DESC
);
