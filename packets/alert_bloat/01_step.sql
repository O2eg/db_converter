WITH btree_index_atts AS (
    SELECT nspname,
        ci.relname as index_name, 
        ci.reltuples, 
        ci.relpages, 
        i.indrelid, i.indexrelid,
        ci.relam,
        ct.relname as tablename,
        regexp_split_to_table(indkey::text, ' ')::smallint AS attnum,
        coalesce(substring(array_to_string(ci.reloptions, ' ') from '%fillfactor=#"__#"%' for '#')::int2, 100) as fillfactor,
        indexrelid as index_oid,
        (SELECT pg_get_indexdef (i.indexrelid)) as def,
        indisunique::integer,
        indisprimary::integer, 
        (select pg_get_constraintdef(cn.oid)) as constraintdef, cn.oid as conoid, 
        conname,
        cn.contype,
        cn.conrelid
    FROM pg_index i
    JOIN pg_class AS ci ON i.indexrelid = ci.oid and ci.relkind = 'i'
    JOIN pg_class AS ct ON i.indrelid = ct.oid and ct.relkind in ('r', 'm', 't')
    -- where relkind: r = ordinary table, i = index, S = sequence, v = view, m = materialized view,
        -- c = composite type, t = TOAST table, f = foreign table
    JOIN pg_namespace n ON n.oid = ci.relnamespace
    JOIN pg_am a ON ci.relam = a.oid
    LEFT JOIN pg_constraint cn ON cn.connamespace = n.oid and cn.conindid = i.indexrelid
    WHERE a.amname = 'btree' and ci.relpages > 3000
         AND nspname NOT IN ('pg_catalog', 'information_schema')
         AND ct.reltuples > 5000
         AND ci.relname not in ('d_last_session_site_id_ip_user_agent')
),
index_item_sizes AS (
    SELECT
        ind_atts.nspname, ind_atts.index_name,
        ind_atts.reltuples, ind_atts.relpages, ind_atts.relam,
        indrelid AS table_oid, index_oid,
        current_setting('block_size')::numeric AS bs, fillfactor,
        8 AS maxalign,
        24 AS pagehdr,
        CASE WHEN max(coalesce(pg_stats.null_frac, 0)) = 0
            THEN 2
            ELSE 6
        END AS index_tuple_hdr,
        sum((1-coalesce(pg_stats.null_frac, 0)) * coalesce(pg_stats.avg_width, 1024)) AS nulldatawidth,
        max(def) as def, max(indisunique) as indisunique, max(indisprimary) as indisprimary,
        max(constraintdef) as constraintdef, max(conoid) as conoid, max(conname) as conname,
        max(contype) as contype, max(conrelid) as conrelid -- ext info
    FROM pg_attribute
    JOIN btree_index_atts AS ind_atts ON pg_attribute.attrelid = ind_atts.indexrelid AND pg_attribute.attnum = ind_atts.attnum
    JOIN pg_stats ON pg_stats.schemaname = ind_atts.nspname
          -- stats for regular index columns
          AND (
            (
               pg_stats.tablename = ind_atts.tablename AND
               pg_stats.attname = pg_catalog.pg_get_indexdef(pg_attribute.attrelid, pg_attribute.attnum, TRUE)
            )
          -- stats for functional indexes
          OR   (pg_stats.tablename = ind_atts.index_name AND pg_stats.attname = pg_attribute.attname))
    WHERE pg_attribute.attnum > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
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
      as expected,
    def, indisunique, indisprimary, constraintdef, conoid, conname, contype, conrelid, fillfactor
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
        stat.idx_scan as index_scans,
        def, indisunique, indisprimary, constraintdef, conoid, conname, contype, conrelid, fillfactor  -- ext info
    FROM index_aligned_est
    JOIN pg_class ON pg_class.oid=index_aligned_est.table_oid
    JOIN pg_stat_user_indexes AS stat ON index_aligned_est.index_oid = stat.indexrelid
)
SELECT
    nspname as schema_name, table_name, index_name,
    round(realbloat::numeric, 2) as bloat_pct,
    pg_size_pretty(wastedbytes::NUMERIC) as bloat_size,
    pg_size_pretty(totalbytes::NUMERIC) as index_size,
    pg_size_pretty(table_bytes::NUMERIC) as table_size,
    fillfactor
FROM raw_bloat
WHERE (round(realbloat::numeric, 2) >= 80 and wastedbytes/(1024^2)::NUMERIC > 5)
ORDER BY wastedbytes DESC nulls last
LIMIT 10;