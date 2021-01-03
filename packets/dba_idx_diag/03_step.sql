-- Issue: no primary key or unique index
-- Solution: create a primary key or unique index
select 'Checking for missing primary key or unique index...' as "Check name";
select
	n.nspname,
	cr.relname as tbl_name
from pg_class cr
join pg_namespace n on n.oid = cr.relnamespace and
	nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
left join pg_index i on cr.oid = i.indrelid and (i.indisprimary or i.indisunique)
left join pg_class ci on ci.oid = i.indexrelid and ci.relkind = 'i'  
where
	cr.relkind = 'r' and
	i.indrelid is null
order by n.nspname, tbl_name
limit 100