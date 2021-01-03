-- Issue: there are duplicate indexes
-- Solution: remove duplicated indexes
select 'Checking duplicated indexes...' as "Check name";
select
	max(n.nspname) as nspname,
	max(cr.relname) as tbl_name,
	array_agg(distinct ci.relname) as idxs
from pg_index i
join pg_class ci on ci.oid = i.indexrelid and ci.relkind = 'i'
join pg_class cr on cr.oid = i.indrelid and cr.relkind = 'r'
join pg_namespace n on n.oid = ci.relnamespace and
	nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
join pg_attribute a on
	  a.attrelid = i.indrelid and i.indkey[1] is not null
	  and (a.attnum = i.indkey[0] or a.attnum = i.indkey[1])
	  and not a.attisdropped
group by cr.oid, i.indkey[0], i.indkey[1]
having count(*) > 2
union all
select
	max(n.nspname) as nspname,
	max(cr.relname) as tbl_name,
	array_agg(distinct ci.relname) as idxs
from pg_index i
join pg_class ci on ci.oid = i.indexrelid and ci.relkind = 'i'
join pg_class cr on cr.oid = i.indrelid and cr.relkind = 'r'
join pg_namespace n on n.oid = ci.relnamespace and
	nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
join pg_attribute a on
	  a.attrelid = i.indrelid
	  and a.attnum = i.indkey[0] and i.indkey[1] is null
	  and not a.attisdropped
group by cr.oid, i.indkey[0]
having count(*) > 1
order by nspname, tbl_name
limit 100