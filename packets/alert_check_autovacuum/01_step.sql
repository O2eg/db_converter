------------------
-- tx start
do $$ 
begin
	if exists(select 1
		from pg_class
		where '{autovacuum_enabled=false}'::text[] @> reloptions and relkind = 'r')
	then
			raise exception 'autovacuum_enabled=false on some tables! %', (
				select array_agg(quote_ident(n.nspname) || '.' || quote_ident(c.relname))::text
				from pg_class c
				join pg_namespace n on n.oid = c.relnamespace
				where '{autovacuum_enabled=false}'::text[] @> reloptions and relkind = 'r'
			);																				   
	end if;
end$$;
-- tx end
------------------