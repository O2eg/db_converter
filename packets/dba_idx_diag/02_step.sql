-- Issue: the index has more than 3 fields
-- Solution: create an index of 2-3 fields and compare the statistics of scans per day "2-3 fields" vs "4+ fields"
select 'Checking indexes with more than 3 fields...' as "Check name";
select
	n.nspname as nspname,
	cr.relname as tbl_name,
	ci.relname as idx_name,
	indnatts as flds_in_idx
from pg_index i
join pg_class ci on ci.oid = i.indexrelid and ci.relkind = 'i'
join pg_class cr on cr.oid = i.indrelid and cr.relkind = 'r'
join pg_namespace n on n.oid = ci.relnamespace and
	nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
where
	indnatts > 3
limit 100