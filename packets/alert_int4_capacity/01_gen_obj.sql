    select
	  '' as maint,
      -- c.oid,
      -- (select spcname from pg_tablespace where oid = reltablespace) as tblspace,
      nspname as schema_name,
      relname as table_name,
      t.typname,
      attname,
      (select pg_get_serial_sequence(quote_ident(nspname) || '.' || quote_ident(relname), attname)) as seq
    from pg_index i
    join pg_class c on c.oid = i.indrelid
    join pg_namespace n on n.oid = c.relnamespace
    join pg_attribute a on
      a.attrelid = i.indrelid
      and a.attnum = any(i.indkey)
    join pg_type t on t.oid = atttypid
    where
      i.indisprimary
	  -- 8192 * 10000 = 81,92MB
	  and c.relpages > 10000
      and t.typname in ('int2', 'int4')
      and nspname <> 'pg_toast'
