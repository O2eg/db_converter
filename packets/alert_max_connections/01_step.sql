SELECT format('Low number of free connections: %s (%s) [max_connections = %s]', T.c, T.v || '%', T.s)
FROM (
	SELECT
		round(((select count(1) from pg_stat_activity) * 100)::numeric / setting::integer, 2) as v,
		setting::integer - (select count(1) from pg_stat_activity) as c,
		setting as s
	FROM pg_settings
	WHERE name = 'max_connections'
) T WHERE T.v > 70