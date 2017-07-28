CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW backend_queries AS (
SELECT pg_stat_get_backend_pid(s.backendid) AS pid,
    pg_stat_get_backend_client_addr(s.backendid) AS client_addr,
    pg_stat_get_backend_client_port(s.backendid) AS client_port,
    pg_stat_get_backend_start(s.backendid) AS session_start_time,
    pg_stat_get_backend_xact_start(s.backendid) AS transaction_start_time,
    now() - pg_stat_get_backend_xact_start(s.backendid) AS transaction_time,
    pg_stat_get_backend_waiting(s.backendid) AS is_waiting,
    regexp_replace(pg_stat_get_backend_activity(s.backendid), E'[\n\r\t ]+', ' ', 'ig') AS query
FROM ( SELECT pg_stat_get_backend_idset() AS backendid) s
WHERE regexp_replace(pg_stat_get_backend_activity(s.backendid), E'[\n\r\t ]+', ' ', 'ig') <> ALL (ARRAY['select 1'::text, 'SELECT $1;'::text, 'BEGIN'::text, 'COMMIT'::text, 'ROLLBACK'::text, 'DISCARD ALL;'::text, 'DEALLOCATE ALL;'::text, 'SHOW TRANSACTION ISOLATION LEVEL'::text, 'SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE'::text])
ORDER BY transaction_time DESC
);
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

-- btree index stats query
-- estimates bloat for btree indexes
CREATE OR REPLACE VIEW bloat_indexes AS (
WITH btree_index_atts AS (
    SELECT nspname, 
        indexclass.relname as index_name, 
        indexclass.reltuples, 
        indexclass.relpages, 
        indrelid, indexrelid,
        indexclass.relam,
        tableclass.relname as tablename,
        regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
        indexrelid as index_oid
    FROM pg_index
    JOIN pg_class AS indexclass ON pg_index.indexrelid = indexclass.oid
    JOIN pg_class AS tableclass ON pg_index.indrelid = tableclass.oid
    JOIN pg_namespace ON pg_namespace.oid = indexclass.relnamespace
    JOIN pg_am ON indexclass.relam = pg_am.oid
    WHERE pg_am.amname = 'btree' and indexclass.relpages > 0
         --AND nspname NOT IN ('pg_catalog','information_schema')
    ),
index_item_sizes AS (
    SELECT
    ind_atts.nspname, ind_atts.index_name, 
    ind_atts.reltuples, ind_atts.relpages, ind_atts.relam,
    indrelid AS table_oid, index_oid,
    current_setting('block_size')::numeric AS bs,
    8 AS maxalign,
    24 AS pagehdr,
    CASE WHEN max(coalesce(pg_stats.null_frac,0)) = 0
        THEN 2
        ELSE 6
    END AS index_tuple_hdr,
    sum( (1-coalesce(pg_stats.null_frac, 0)) * coalesce(pg_stats.avg_width, 1024) ) AS nulldatawidth
    FROM pg_attribute
    JOIN btree_index_atts AS ind_atts ON pg_attribute.attrelid = ind_atts.indexrelid -- AND pg_attribute.attnum = ind_atts.attnum
    JOIN pg_stats ON pg_stats.schemaname = ind_atts.nspname
          -- stats for regular index columns
          AND ( (pg_stats.tablename = ind_atts.tablename AND pg_stats.attname = pg_catalog.pg_get_indexdef(pg_attribute.attrelid, pg_attribute.attnum, TRUE)) 
          -- stats for functional indexes
          OR   (pg_stats.tablename = ind_atts.index_name AND pg_stats.attname = pg_attribute.attname))
    WHERE pg_attribute.attnum > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
index_aligned_est AS (
    SELECT maxalign, bs, nspname, index_name, reltuples,
        relpages, relam, table_oid, index_oid,
        coalesce (
            ceil (
                reltuples * ( 6 
                    + maxalign 
                    - CASE
                        WHEN index_tuple_hdr%maxalign = 0 THEN maxalign
                        ELSE index_tuple_hdr%maxalign
                      END
                    + nulldatawidth 
                    + maxalign 
                    - CASE /* Add padding to the data to align on MAXALIGN */
                        WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign
                        ELSE nulldatawidth::integer%maxalign
                      END
                )::numeric 
              / ( bs - pagehdr::NUMERIC )
              +1 )
         , 0 )
      as expected
    FROM index_item_sizes
),
raw_bloat AS (
    SELECT current_database() as dbname, nspname, pg_class.relname AS table_name, index_name,
        bs*(index_aligned_est.relpages)::bigint AS totalbytes, expected,
        CASE
            WHEN index_aligned_est.relpages <= expected 
                THEN 0
                ELSE bs*(index_aligned_est.relpages-expected)::bigint 
            END AS wastedbytes,
        CASE
            WHEN index_aligned_est.relpages <= expected
                THEN 0 
                ELSE bs*(index_aligned_est.relpages-expected)::bigint * 100 / (bs*(index_aligned_est.relpages)::bigint) 
            END AS realbloat,
        pg_relation_size(index_aligned_est.table_oid) as table_bytes,
        stat.idx_scan as index_scans
    FROM index_aligned_est
    JOIN pg_class ON pg_class.oid=index_aligned_est.table_oid
    JOIN pg_stat_user_indexes AS stat ON index_aligned_est.index_oid = stat.indexrelid
),
format_bloat AS (
SELECT dbname as database_name, nspname as schema_name, table_name, index_name,
        round(realbloat) as bloat_pct, round(wastedbytes/(1024^2)::NUMERIC) as bloat_mb,
        round(totalbytes/(1024^2)::NUMERIC,3) as index_mb,
        round(table_bytes/(1024^2)::NUMERIC,3) as table_mb,
        index_scans
FROM raw_bloat
)
-- final query outputting the bloated indexes
SELECT database_name, schema_name, table_name, index_name, index_scans, bloat_pct, bloat_mb, index_mb, table_mb
FROM format_bloat
ORDER BY bloat_mb, bloat_pct DESC 
);
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

-- new table bloat query
-- still needs work; is often off by +/- 20%
CREATE OR REPLACE VIEW bloat_tables AS (
WITH constants AS (
    SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 8 AS ma
),
no_stats AS (
    SELECT table_schema, table_name, 
        n_live_tup::numeric as est_rows,
        pg_table_size(relid)::numeric as table_size
    FROM information_schema.columns
        JOIN pg_stat_user_tables as psut
           ON table_schema = psut.schemaname
           AND table_name = psut.relname
        LEFT OUTER JOIN pg_stats
        ON table_schema = pg_stats.schemaname
            AND table_name = pg_stats.tablename
            AND column_name = attname 
    WHERE attname IS NULL
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
    GROUP BY table_schema, table_name, relid, n_live_tup
),
null_headers AS (
    -- calculate null header sizes
    -- omitting tables which dont have complete stats
    -- and attributes which aren't visible
    SELECT
        hdr+1+(sum(CASE WHEN null_frac <> 0 THEN 1 else 0 END)/8) as nullhdr,
        SUM((1-null_frac)*avg_width) as datawidth,
        MAX(null_frac) as maxfracsum,
        schemaname,
        tablename,
        hdr, ma, bs
    FROM pg_stats CROSS JOIN constants
        LEFT OUTER JOIN no_stats
            ON schemaname = no_stats.table_schema
            AND tablename = no_stats.table_name
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        AND no_stats.table_name IS NULL
        AND EXISTS ( SELECT 1
            FROM information_schema.columns
                WHERE schemaname = columns.table_schema
                    AND tablename = columns.table_name )
    GROUP BY schemaname, tablename, hdr, ma, bs
),
data_headers AS (
    -- estimate header and row size
    SELECT
        ma, bs, hdr, schemaname, tablename,
        (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
        (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
    FROM null_headers
),
table_estimates AS (
    -- make estimates of how large the table should be
    -- based on row and page size
    SELECT schemaname, tablename, bs,
        reltuples::numeric as est_rows, relpages * bs as table_bytes,
    CEIL((reltuples*
            (datahdr + nullhdr2 + 4 + ma -
                (CASE WHEN datahdr%ma=0
                    THEN ma ELSE datahdr%ma END)
                )/(bs-20))) * bs AS expected_bytes,
        reltoastrelid
    FROM data_headers
        JOIN pg_class ON tablename = relname
        JOIN pg_namespace ON relnamespace = pg_namespace.oid
            AND schemaname = nspname
    WHERE pg_class.relkind = 'r'
),
estimates_with_toast AS (
    -- add in estimated TOAST table sizes
    -- estimate based on 4 toast tuples per page because we dont have 
    -- anything better.  also append the no_data tables
    SELECT schemaname, tablename, 
        TRUE as can_estimate,
        est_rows,
        table_bytes + ( coalesce(toast.relpages, 0) * bs ) as table_bytes,
        expected_bytes + ( ceil( coalesce(toast.reltuples, 0) / 4 ) * bs ) as expected_bytes
    FROM table_estimates LEFT OUTER JOIN pg_class as toast
        ON table_estimates.reltoastrelid = toast.oid
            AND toast.relkind = 't'
),
table_estimates_plus AS (
-- add some extra metadata to the table data
-- and calculations to be reused
-- including whether we cant estimate it
-- or whether we think it might be compressed
    SELECT current_database() as databasename,
            schemaname, tablename, can_estimate, 
            est_rows,
            CASE
                WHEN table_bytes > 0
                THEN table_bytes::NUMERIC
                ELSE NULL::NUMERIC 
            END AS table_bytes,
            CASE
                WHEN expected_bytes > 0 
                THEN expected_bytes::NUMERIC
                ELSE NULL::NUMERIC 
            END AS expected_bytes,
            CASE
                WHEN expected_bytes > 0 AND table_bytes > 0
                    AND expected_bytes <= table_bytes
                THEN (table_bytes - expected_bytes)::NUMERIC
                ELSE 0::NUMERIC
            END AS bloat_bytes
    FROM estimates_with_toast
    UNION ALL
    SELECT current_database() as databasename, 
        table_schema, table_name, FALSE, 
        est_rows, table_size,
        NULL::NUMERIC, NULL::NUMERIC
    FROM no_stats
),
bloat_data AS (
    -- do final math calculations and formatting
    select current_database() as database_name,
        schemaname as schema_name, tablename as table_name, can_estimate, 
        table_bytes, round(table_bytes/(1024^2)::NUMERIC,3) as table_mb,
        expected_bytes, round(expected_bytes/(1024^2)::NUMERIC,3) as expected_mb,
        round(bloat_bytes*100/table_bytes) as bloat_pct,
        round(bloat_bytes/(1024::NUMERIC^2),2) as bloat_mb,
        est_rows
    FROM table_estimates_plus
)
SELECT database_name, schema_name, table_name, bloat_pct, bloat_mb, table_mb, can_estimate, est_rows
FROM bloat_data
ORDER BY bloat_mb, bloat_pct DESC
);
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
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW duplicate_indexes_fuzzy AS (
WITH index_cols_ord as (
    SELECT attrelid, attnum, attname
    FROM pg_attribute
        JOIN pg_index ON indexrelid = attrelid
    WHERE indkey[0] > 0
    ORDER BY attrelid, attnum
),
index_col_list AS (
    SELECT attrelid,
        array_agg(attname) as cols
    FROM index_cols_ord
    GROUP BY attrelid
),
dup_natts AS (
SELECT indrelid, indexrelid
FROM pg_index as ind
WHERE EXISTS ( SELECT 1
    FROM pg_index as ind2
    WHERE ind.indrelid = ind2.indrelid
    AND ( ind.indkey @> ind2.indkey
     OR ind.indkey <@ ind2.indkey )
    AND ind.indkey[0] = ind2.indkey[0]
    AND ind.indkey <> ind2.indkey
    AND ind.indexrelid <> ind2.indexrelid
) )
SELECT userdex.schemaname as schema_name,
    userdex.relname as table_name,
    userdex.indexrelname as index_name,
    array_to_string(cols, ', ') as index_cols,
    indexdef,
    idx_scan as index_scans
FROM pg_stat_user_indexes as userdex
    JOIN index_col_list ON index_col_list.attrelid = userdex.indexrelid
    JOIN dup_natts ON userdex.indexrelid = dup_natts.indexrelid
    JOIN pg_indexes ON userdex.schemaname = pg_indexes.schemaname
        AND userdex.indexrelname = pg_indexes.indexname
ORDER BY userdex.schemaname, userdex.relname, cols, userdex.indexrelname
);
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW fk_no_index AS (
WITH fk_actions ( code, action ) AS (
    VALUES ( 'a', 'error' ),
        ( 'r', 'restrict' ),
        ( 'c', 'cascade' ),
        ( 'n', 'set null' ),
        ( 'd', 'set default' )
),
fk_list AS (
    SELECT pg_constraint.oid as fkoid, conrelid, confrelid as parentid,
        conname, relname, nspname,
        fk_actions_update.action as update_action,
        fk_actions_delete.action as delete_action,
        conkey as key_cols
    FROM pg_constraint
        JOIN pg_class ON conrelid = pg_class.oid
        JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
        JOIN fk_actions AS fk_actions_update ON confupdtype = fk_actions_update.code
        JOIN fk_actions AS fk_actions_delete ON confdeltype = fk_actions_delete.code
    WHERE contype = 'f'
),
fk_attributes AS (
    SELECT fkoid, conrelid, attname, attnum
    FROM fk_list
        JOIN pg_attribute
            ON conrelid = attrelid
            AND attnum = ANY( key_cols )
    ORDER BY fkoid, attnum
),
fk_cols_list AS (
    SELECT fkoid, array_agg(attname) as cols_list
    FROM fk_attributes
    GROUP BY fkoid
),
index_list AS (
    SELECT indexrelid as indexid,
        pg_class.relname as indexname,
        indrelid,
        indkey,
        indpred is not null as has_predicate,
        pg_get_indexdef(indexrelid) as indexdef
    FROM pg_index
        JOIN pg_class ON indexrelid = pg_class.oid
    WHERE indisvalid
),
fk_index_match AS (
    SELECT fk_list.*,
        indexid,
        indexname,
        indkey::int[] as indexatts,
        has_predicate,
        indexdef,
        array_length(key_cols, 1) as fk_colcount,
        array_length(indkey,1) as index_colcount,
        round(pg_relation_size(conrelid)/(1024^2)::numeric) as table_mb,
        cols_list
    FROM fk_list
        JOIN fk_cols_list USING (fkoid)
        LEFT OUTER JOIN index_list
            ON conrelid = indrelid
            AND (indkey::int2[])[0:(array_length(key_cols,1) -1)] @> key_cols

),
fk_perfect_match AS (
    SELECT fkoid
    FROM fk_index_match
    WHERE (index_colcount - 1) <= fk_colcount
        AND NOT has_predicate
        AND indexdef LIKE '%USING btree%'
),
fk_index_check AS (
    SELECT 'no index' as issue, *, 1 as issue_sort
    FROM fk_index_match
    WHERE indexid IS NULL
    UNION ALL
    SELECT 'questionable index' as issue, *, 2
    FROM fk_index_match
    WHERE indexid IS NOT NULL
        AND fkoid NOT IN (
            SELECT fkoid
            FROM fk_perfect_match)
),
parent_table_stats AS (
    SELECT fkoid, tabstats.relname as parent_name,
        (n_tup_ins + n_tup_upd + n_tup_del + n_tup_hot_upd) as parent_writes,
        round(pg_relation_size(parentid)/(1024^2)::numeric) as parent_mb
    FROM pg_stat_user_tables AS tabstats
        JOIN fk_list
            ON relid = parentid
),
fk_table_stats AS (
    SELECT fkoid,
        (n_tup_ins + n_tup_upd + n_tup_del + n_tup_hot_upd) as writes,
        seq_scan as table_scans
    FROM pg_stat_user_tables AS tabstats
        JOIN fk_list
            ON relid = conrelid
)
SELECT nspname as schema_name,
    relname as table_name,
    conname as fk_name,
    issue,
    table_mb,
    writes,
    table_scans,
    parent_name,
    parent_mb,
    parent_writes,
    cols_list,
    indexdef
FROM fk_index_check
    JOIN parent_table_stats USING (fkoid)
    JOIN fk_table_stats USING (fkoid)
WHERE table_mb > 9
    AND ( writes > 1000
        OR parent_writes > 1000
        OR parent_mb > 10 )
ORDER BY issue_sort, table_mb DESC, table_name, fk_name
);
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
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW lock_check AS (
SELECT
	wait.pid,
	wait.query as q,
	wait.locktype,
	granted.pid as waitfor_pid,
	granted.relation,
	granted.datname || '.' || d.nspname || '.' || c.relname as name,
	granted.transactionid,
	granted.virtualxid,
	granted.usename,
	granted.client_addr,
	granted.query
FROM
	(SELECT
		a.query,
		b.pid,
		b.relation,
		b.transactionid,
		b.page,
		b.tuple,
		b.locktype,
		b.virtualxid
	FROM
		pg_stat_activity a,
		pg_locks b
	WHERE
		a.waiting = 't'
		AND a.pid = b.pid
		AND granted = 'f'
) wait
INNER JOIN
	(SELECT
		b.pid,
		b.usename,
		b.client_addr,
		b.backend_start,
		b.query_start,
		b.waiting,
		b.query,
		b.datname,
		a.relation,
		a.transactionid,
		a.page,
		a.tuple,
		a.locktype,
		a.virtualxid
	FROM
		pg_locks a,
		pg_stat_activity b
	WHERE
		a.pid = b.pid
		AND a.granted = 't'
	) granted
ON (
	( wait.locktype = 'transactionid'
	AND granted.locktype = 'transactionid'
	AND wait.transactionid = granted.transactionid )
	OR
	( wait.locktype = 'relation'
	AND granted.locktype = 'relation'
	AND wait.relation = granted.relation
	)
	OR
	( wait.locktype = 'virtualxid'
	AND granted.locktype = 'virtualxid'
	AND wait.virtualxid = granted.virtualxid )
	OR
	( wait.locktype = 'tuple'
	AND granted.locktype = 'tuple'
	AND wait.relation = granted.relation
	AND wait.page = granted.page
	AND wait.tuple = granted.tuple )
	)
LEFT JOIN
	pg_class c
ON ( c.relfilenode = wait.relation )
LEFT JOIN
	pg_namespace d
ON ( c.relnamespace = d.oid )
);

CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

-- do statement to set things up for transaction lock logging
-- written so that it can be run repeatedly

DO $d$
BEGIN
    PERFORM 1
    FROM pg_stat_user_tables
    WHERE relname = 'log_table_locks';
    SET search_path = pgtools, public;

    IF NOT FOUND THEN

        CREATE TABLE log_table_locks (
            lock_ts TIMESTAMPTZ,
            waiting_pid INT,
            wait_xid TEXT,
            locked_pid INT,
            locked_xid TEXT,
            locked_relation TEXT,
            waiting_type TEXT,
            waiting_mode TEXT,
            waiting_tuple TEXT,
            locked_type TEXT,
            locked_mode TEXT,
            locked_tuple TEXT,
            waiting_app TEXT,
            waiting_addr TEXT,
            waiting_xact_start TIMESTAMPTZ,
            waiting_query_start TIMESTAMPTZ,
            waiting_start TIMESTAMPTZ,
            waiting_query TEXT,
            locked_app TEXT,
            locked_addr TEXT,
            locked_xact_start TIMESTAMPTZ,
            locked_query_start TIMESTAMPTZ,
            locked_state TEXT,
            locked_state_start TIMESTAMPTZ,
            locked_last_query TEXT
        );

        CREATE OR REPLACE FUNCTION log_table_locks()
        RETURNS BIGINT
        LANGUAGE sql
        SET statement_timeout = '2s'
        AS $f$
            WITH table_locks AS (
                select pid,
                relation::int as relation,
                (relation::regclass)::text as locked_relation,
                mode,
                page || ':' || tuple as locked_tuple,
                locktype,
                coalesce(transactionid::text, virtualxid) as lxid,
                granted
                from pg_locks
                    join pg_database
                        ON pg_locks.database = pg_database.oid
                where relation is not null
                    and pg_database.datname = current_database()
                    and locktype IN ( 'relation', 'extend', 'page', 'tuple' )
            ),
            lock_granted AS (
                select * from table_locks
                where granted
            ),
            lock_waiting AS (
                select * from table_locks
                where not granted
            ),
            inserter as (
            INSERT INTO log_table_locks
            select now() as lock_ts,
                lock_waiting.pid as waiting_pid,
                lock_waiting.lxid as wait_xid,
                lock_granted.pid as locked_pid,
                lock_granted.lxid as locked_xid,
                lock_granted.locked_relation,
                lock_waiting.locktype as waiting_type,
                lock_waiting.mode as waiting_mode,
                lock_waiting.locked_tuple as tuple_waiting,
                lock_granted.locktype as locked_type,
                lock_granted.mode as lock_mode,
                lock_granted.locked_tuple as tuple_locked,
                waiting_proc.application_name as waiting_app,
                waiting_proc.client_addr as waiting_addr,
                waiting_proc.xact_start as waiting_xact_start,
                waiting_proc.query_start as waiting_query_start,
                waiting_proc.state_change as waiting_start,
                waiting_proc.query as waiting_query,
                locked_proc.application_name as locked_app,
                locked_proc.client_addr as locked_addr,
                locked_proc.xact_start as locked_xact_start,
                locked_proc.query_start as locked_query_start,
                locked_proc.state as locked_state,
                locked_proc.state_change as locked_state_start,
                locked_proc.query as locked_last_query
            from lock_waiting
                JOIN pg_stat_activity as waiting_proc
                    ON lock_waiting.pid = waiting_proc.pid
                LEFT OUTER JOIN lock_granted
                    ON lock_waiting.relation = lock_granted.relation
                LEFT OUTER JOIN pg_stat_activity as locked_proc
                    ON lock_granted.pid = locked_proc.pid
            order by locked_pid, locked_relation
            returning waiting_pid
            )
            SELECT count(*)
            FROM inserter;
        $f$;

    END IF;
END;
$d$;
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

-- do statement to set things up for transaction lock logging
-- written so that it can be run repeatedly

DO $f$
BEGIN
    PERFORM 1
    FROM pg_stat_user_tables
    WHERE relname = 'log_transaction_locks';
    SET search_path = pgtools, public;

    IF NOT FOUND THEN

        CREATE TABLE log_transaction_locks (
            lock_ts TIMESTAMPTZ,
            waiting_pid INT,
            waiting_xid TEXT,
            locked_pid INT,
            waiting_app TEXT,
            waiting_addr TEXT,
            waiting_xact_start TIMESTAMPTZ,
            waiting_query_start TIMESTAMPTZ,
            waiting_start TIMESTAMPTZ,
            waiting_query TEXT,
            locked_app TEXT,
            locked_addr TEXT,
            locked_xact_start TIMESTAMPTZ,
            locked_query_start TIMESTAMPTZ,
            locked_state TEXT,
            locked_state_start TIMESTAMPTZ,
            locked_last_query TEXT,
            waiting_relations TEXT[],
            waiting_modes TEXT[],
            waiting_lock_types TEXT[],
            locked_relations TEXT[],
            locked_modes TEXT[],
            locked_lock_types TEXT[]
        );

        CREATE OR REPLACE FUNCTION log_transaction_locks()
        RETURNS BIGINT
        LANGUAGE sql
        SET statement_timeout = '2s'
        AS $l$
        WITH mylocks AS (
            SELECT * FROM pg_locks
            WHERE locktype IN ( 'transactionid', 'virtualxid' )
        ),
        table_locks AS (
            select pid,
            (relation::regclass)::TEXT as lockobj,
            case when page is not null and tuple is not null then
                mode || ' on ' || page::text || ':' || tuple::text
            else
                mode
            end as lock_mode,
            locktype
            from mylocks
                join pg_database
                    ON mylocks.database = pg_database.oid
            where relation is not null
                and pg_database.datname = current_database()
            order by lockobj
        ),
        locked_list AS (
            select pid,
            array_agg(lockobj) as lock_relations,
            array_agg(lock_mode) as lock_modes,
            array_agg(locktype) as lock_types
            from table_locks
            group by pid
        ),
        txn_locks AS (
            select pid, transactionid::text as lxid, granted
            from mylocks
            where locktype = 'transactionid'
            union all
            select pid, virtualxid::text as lxid, granted
            from mylocks
            where locktype = 'virtualxid'
        ),
        txn_granted AS (
            select pid, lxid from txn_locks
            where granted
        ),
        txn_waiting AS (
            select pid, lxid from txn_locks
            where not granted
        ),
        inserter as (
            insert into log_transaction_locks
            select now() as lock_ts,
                txn_waiting.pid as waiting_pid,
                txn_waiting.lxid as wait_xid,
                txn_granted.pid as locked_pid,
                waiting_proc.application_name as waiting_app,
                waiting_proc.client_addr as waiting_addr,
                waiting_proc.xact_start as waiting_xact_start,
                waiting_proc.query_start as waiting_query_start,
                waiting_proc.state_change as waiting_start,
                waiting_proc.query as waiting_query,
                locked_proc.application_name as locked_app,
                locked_proc.client_addr as locked_addr,
                locked_proc.xact_start as locked_xact_start,
                locked_proc.query_start as locked_query_start,
                locked_proc.state as locked_state,
                locked_proc.state_change as locked_state_start,
                locked_proc.query as locked_last_query,
                waiting_locks.lock_relations as waiting_relations,
                waiting_locks.lock_modes as waiting_modes,
                waiting_locks.lock_types as waiting_lock_types,
                locked_locks.lock_relations as locked_relations,
                locked_locks.lock_modes as locked_modes,
                locked_locks.lock_types as locked_lock_types
            from txn_waiting
                JOIN pg_stat_activity as waiting_proc
                    ON txn_waiting.pid = waiting_proc.pid
                LEFT OUTER JOIN txn_granted
                    ON txn_waiting.lxid = txn_granted.lxid
                LEFT OUTER JOIN pg_stat_activity as locked_proc
                    ON txn_granted.pid = locked_proc.pid
                LEFT OUTER JOIN locked_list AS waiting_locks
                    ON txn_waiting.pid = waiting_locks.pid
                LEFT OUTER JOIN locked_list AS locked_locks
                    ON txn_granted.pid = locked_locks.pid
            returning waiting_pid
        )
        SELECT COUNT(*) from inserter;
        $l$;
        
    END IF;
END;
$f$;
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW no_stats_table AS (
  SELECT table_schema, table_name,
      ( pg_class.relpages = 0 ) AS is_empty,
      ( psut.relname IS NULL OR ( psut.last_analyze IS NULL and psut.last_autoanalyze IS NULL ) ) AS never_analyzed,
      array_agg(column_name::TEXT) as no_stats_columns
  FROM information_schema.columns
      JOIN pg_class ON columns.table_name = pg_class.relname
          AND pg_class.relkind = 'r'
      JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
          AND nspname = table_schema
      LEFT OUTER JOIN pg_stats
      ON table_schema = pg_stats.schemaname
          AND table_name = pg_stats.tablename
          AND column_name = pg_stats.attname
      LEFT OUTER JOIN pg_stat_user_tables AS psut
          ON table_schema = psut.schemaname
          AND table_name = psut.relname
  WHERE pg_stats.attname IS NULL
      AND table_schema NOT IN ('pg_catalog', 'information_schema')
  GROUP BY table_schema, table_name, relpages, psut.relname, last_analyze, last_autoanalyze
);
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
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW streaming_timedelay AS (
SELECT
    CASE
        WHEN (pg_last_xlog_receive_location() = pg_last_xlog_replay_location()) THEN (0)::double precision
        ELSE date_part('epoch'::text, (now() - pg_last_xact_replay_timestamp()))
    END AS time_delay
);
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

--html decode funtion
CREATE OR REPLACE FUNCTION strip_tags(TEXT)
RETURNS TEXT
AS $$
    SELECT regexp_replace(regexp_replace($1, E'(?x)<[^>]*?(\s alt \s* = \s* ([\'"]) ([^>]*?) \2) [^>]*? >', E'\3'), E'(?x)(< [^>]*? >)', '', 'g');
$$
LANGUAGE SQL;
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
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

--url decode function
CREATE OR REPLACE FUNCTION url_decode(encode_url text)
RETURNS text
IMMUTABLE STRICT AS $$
DECLARE
    bin bytea = '';
    byte text;
BEGIN
    FOR byte IN (select (regexp_matches(encode_url, '(%..|.)', 'g'))[1]) LOOP
        IF length(byte) = 3 THEN
            bin = bin || decode(substring(byte, 2, 2), 'hex');
        ELSE
            bin = bin || byte::bytea;
        END IF;
    END LOOP;
RETURN convert_from(bin, 'utf8');
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION decode_url(encode_url character varying) 
RETURNS character varying 
IMMUTABLE STRICT AS $$ 
    select convert_from(CasT(E'\\x' || string_agg(CasE WHEN length(r.m[1]) = 1 THEN encode(convert_to(r.m[1], 'SQL_asCII'), 'hex') ELSE substring(r.m[1] from 2 for 2) END, '') as bytea), 'UTF8') 
        from regexp_matches($1, '%[0-9a-f][0-9a-f]|.', 'gi') as r(m); 
$$ 
LANGUAGE SQL; 
CREATE SCHEMA IF NOT EXISTS pgtools;
SET search_path = pgtools, public;

CREATE OR REPLACE VIEW vacuum_needed AS (
  SELECT av.nspname AS schema_name, av.relname AS table_name, av.n_tup_ins, av.n_tup_upd, av.n_tup_del,
      av.hot_update_ratio, av.n_live_tup, av.n_dead_tup, av.reltuples,
      av.athreshold, av.last_vacuum, av.last_analyze,
      av.n_dead_tup::double precision > av.athreshold AS aneeded,
      CASE
          WHEN av.reltuples > 0::double precision THEN round((100.0 * av.n_dead_tup::numeric)::double precision / av.reltuples)
          ELSE 0::double precision
      END AS pct_dead
  FROM ( SELECT n.nspname, c.relname,
              pg_stat_get_tuples_inserted(c.oid) AS n_tup_ins,
              pg_stat_get_tuples_updated(c.oid) AS n_tup_upd,
              pg_stat_get_tuples_deleted(c.oid) AS n_tup_del,
              CASE
                  WHEN pg_stat_get_tuples_updated(c.oid) > 0 THEN pg_stat_get_tuples_hot_updated(c.oid)::real / pg_stat_get_tuples_updated(c.oid)::double precision
                  ELSE 0::double precision
              END AS hot_update_ratio,
              pg_stat_get_live_tuples(c.oid) AS n_live_tup,
              pg_stat_get_dead_tuples(c.oid) AS n_dead_tup, c.reltuples,
              round(current_setting('autovacuum_vacuum_threshold'::text)::integer::double precision + current_setting('autovacuum_vacuum_scale_factor'::text)::numeric::double precision * c.reltuples) AS athreshold,
              date_trunc('minute'::text, GREATEST(pg_stat_get_last_vacuum_time(c.oid), pg_stat_get_last_autovacuum_time(c.oid))) AS last_vacuum,
              date_trunc('minute'::text, GREATEST(pg_stat_get_last_analyze_time(c.oid), pg_stat_get_last_analyze_time(c.oid))) AS last_analyze
          FROM pg_class c
          LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE (c.relkind = ANY (ARRAY['r'::"char", 't'::"char"])) AND n.nspname !~ '^pg_toast'::text
      ) av
  ORDER BY av.n_dead_tup::double precision > av.athreshold DESC, av.n_dead_tup DESC
);
