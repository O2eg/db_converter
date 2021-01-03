-- Issue: the btree index is created on field of text type
-- Solution: replace "btree" with "hash" if there is no partial match
select 'Checking btree indexes created on text fields...' as "Check name";
select
	n.nspname as nspname,
	cr.relname as tbl_name,
	ci.relname as idx_name,
	attname as fld_name,
	pg_get_indexdef(i.indexrelid)
from pg_index i
join pg_class ci on ci.oid = i.indexrelid and ci.relkind = 'i'
join pg_class cr on cr.oid = i.indrelid and cr.relkind = 'r'
join pg_namespace n on n.oid = ci.relnamespace and
	nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
join pg_attribute a on
	  a.attrelid = i.indrelid and a.attnum = any(i.indkey) and not a.attisdropped
join pg_type t on t.oid = atttypid
join pg_am am ON ci.relam = am.oid
where
	t.typname in ('varchar', 'text') and
	am.amname = 'btree'
order by nspname, tbl_name, idx_name
limit 100