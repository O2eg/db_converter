select n.nspname, c.relname as tbl, ci.relname as idx, indisvalid, indisready
from pg_index i
join pg_class c on i.indrelid = c.oid and c.relkind in ('r', 'm')
join pg_class ci on i.indexrelid = ci.oid and ci.relkind = 'i'
join pg_namespace n on n.oid = c.relnamespace
where indisvalid = false or indisready = false
order by c.relpages desc
limit 100