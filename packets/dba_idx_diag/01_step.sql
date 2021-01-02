-- Issue: size of the index is more than 50% of table
-- Solution: needs to check bloat and field types that are indexed
select
	pg_size_pretty(pg_relation_size(cr.oid)) as tbl_size,
	pg_size_pretty(pg_relation_size(ci.oid)) as idx_size,
	round(ci.relpages::decimal/cr.relpages*100, 2) as idx_ratio,
	n.nspname,
	cr.relname as tbl_name,
	ci.relname as idx_name
from pg_index i
join pg_class ci on ci.oid = i.indexrelid and ci.relkind = 'i'
join pg_class cr on cr.oid = i.indrelid and cr.relkind = 'r'
join pg_namespace n on n.oid = ci.relnamespace
	and nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
where
	cr.relpages > 100 and
	ci.relpages > 0 and
	ci.relpages::decimal/cr.relpages > 0.5
order by idx_ratio desc
limit 100