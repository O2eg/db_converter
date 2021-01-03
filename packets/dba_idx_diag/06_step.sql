-- Issue: there is no index on the fields declared as FK
-- Solution: create an index
select
	n_target.nspname as target_nspname,
	c_target.relname as target_relname,
	af.attname as target_fld,
	con.conname,
	n_source.nspname as source_nspname,
	c_source.relname as source_relname,
	ar.attname as source_fld,
	pg_get_constraintdef(con.oid),
	format('CREATE INDEX ON %I.%I USING btree (%I)', n_source.nspname, c_source.relname, ar.attname),
	 ar.attname --,
	 --existed_idxs.oid
from pg_constraint con
join pg_class c_target ON con.confrelid = c_target.oid
join pg_class c_source ON con.conrelid = c_source.oid
join pg_namespace n_target ON n_target.oid = c_target.relnamespace
join pg_namespace n_source ON n_source.oid = c_source.relnamespace
join pg_attribute af on
	  af.attrelid = con.confrelid and af.attnum = any(con.confkey) and not af.attisdropped
join pg_attribute ar on
	  ar.attrelid = con.conrelid and ar.attnum = any(con.conkey) and not ar.attisdropped
left join lateral (
		select
			cr.oid
		from pg_index i
		join pg_class ci on ci.oid = i.indexrelid and ci.relkind = 'i'
		join pg_class cr on cr.oid = i.indrelid and cr.relkind = 'r'
		join pg_namespace n on n.oid = ci.relnamespace and
			nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
		join pg_attribute aidx on aidx.attrelid = i.indrelid and aidx.attnum = i.indkey[0] and not aidx.attisdropped
		where cr.oid = c_source.oid and aidx.attname = ar.attname
) t on true
WHERE contype = 'f' and (confupdtype <> 'a' or confdeltype <> 'a') -- "foreign" exclude "no action"
	and t.oid is null
order by 1, 2, 3, 4, 5, 6, 7, 8