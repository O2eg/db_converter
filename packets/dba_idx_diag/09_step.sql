-- Issue: tables contains more than 1000 blocks and contains no indexes
-- Solution: create indexes according to frequent queries
select 'Checking for tables without indexes...' as "Check name";
select
	n.nspname,
	c.relname as tbl_name,
	pg_size_pretty(pg_relation_size(c.oid)) as tbl_size,
	seq_scan,
	seq_tup_read
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
	and nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
join pg_stat_all_tables sat on sat.relid = c.oid
where
	c.relpages > 1000 and
	seq_scan > 1000 and
	relhasindex is false
limit 100