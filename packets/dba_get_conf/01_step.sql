SELECT
	name,
	setting AS value,
	(
		CASE
		WHEN unit = '8kB' THEN 
			pg_size_pretty(setting::bigint * 1024 * 8)
		WHEN unit = 'kB' AND setting <> '-1' THEN 
			pg_size_pretty(setting::bigint * 1024)
		ELSE ''
		END
	) AS pretty_value,
	boot_val,
	unit
	--category,
	--short_desc,
	--vartype,
FROM pg_settings
WHERE name in (
	'max_connections',
	'shared_buffers',
	'work_mem',
	'autovacuum_work_mem',
	'autovacuum_vacuum_cost_limit',
	'autovacuum_max_workers',
	'checkpoint_timeout',
	'max_wal_size',
	'autovacuum_naptime',
	'autovacuum_work_mem',
	'maintenance_work_mem'
)
ORDER BY name ASC