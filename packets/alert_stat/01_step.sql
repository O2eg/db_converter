select n.nspname,
	c.relname,
	c.reltuples::bigint,
	st.last_autoanalyze,
	st.last_autovacuum,
	st.last_analyze,
	st.last_vacuum
from pg_class c
join pg_namespace n on c.relnamespace = n.oid
left join pg_stat_all_tables st on st.relid = c.oid
where c.reltuples > 1000 and c.relkind in ('r', 'm') and not(n.nspname = 'pg_catalog' and c.relname = 'pg_shdepend') and
(
	(st.last_autoanalyze is null and st.last_autovacuum is null and st.last_analyze is null and st.last_vacuum is null)     -- attention!
	OR
	(
		n.nspname not in('pg_toast', 'pg_catalog')
		AND
		LEAST(
			age(now(), coalesce(st.last_autoanalyze, '2000-01-01'::timestamp with time zone)),
			age(now(), coalesce(st.last_analyze, '2000-01-01'::timestamp with time zone))
		) > '15 days'
		AND
		LEAST(
			age(now(), coalesce(st.last_autovacuum, '2000-01-01'::timestamp with time zone)),
			age(now(), coalesce(st.last_vacuum, '2000-01-01'::timestamp with time zone))
		) > '15 days'
	)
)
order by
	st.last_autoanalyze asc nulls first,
	st.last_autovacuum asc nulls first,
	st.last_analyze asc nulls first,
	st.last_vacuum asc nulls first
limit 10;