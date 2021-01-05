-- Issue: tables with potentially missing indexes
-- Solution: create indexes according to frequent queries or optimize queries
select 'Checking for tables with missed indexes...' as "Check name";
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
	seq_tup_read/seq_scan > 1000 and
	idx_tup_fetch < seq_tup_read and
	relhasindex is true
limit 100