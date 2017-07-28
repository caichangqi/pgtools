CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW function_stats AS (
WITH total AS (
    SELECT SUM(total_time)::BIGINT AS total_agg
    FROM pg_stat_user_functions
)
SELECT
    schemaname AS schema_name,
    funcname AS function_name,
    SUM(calls) AS calls,
    SUM(total_time)::BIGINT AS total_time,
    SUM(self_time)::BIGINT AS self_call_time,
    SUM(total_time)/SUM(calls) AS avg_time,
    100*SUM(total_time)/(SELECT total_agg FROM total) AS pct_functions
FROM pg_stat_user_functions
GROUP BY schema_name, function_name
ORDER BY total_time DESC
);
