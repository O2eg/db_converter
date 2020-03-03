SELECT
	T.nspname,
	T.relname,
	T.size,
	T.idxs_size,
	T.total,
	-- T.total_raw,
	T.n_live_tup,
	T.n_dead_tup,
	T.n_tup_ins,
	T.n_tup_upd
	-- T.n_tup_hot_upd,
	-- T.rel_oid,
	-- T.schema_oid
FROM (
		WITH pg_class_prep AS (
		 SELECT c_1.relname,
			c_1.relnamespace,
			c_1.relkind,
			c_1.oid,
			s.n_live_tup,
			s.n_dead_tup,
			s.n_tup_ins,
			s.n_tup_upd,
			s.n_tup_hot_upd
			FROM pg_class c_1
		JOIN pg_stat_all_tables s ON c_1.oid = s.relid
		WHERE c_1.relpages > 10 AND c_1.relkind = 'r'
		ORDER BY c_1.relpages DESC
		LIMIT 50
	)
	SELECT n.nspname,
		c.relname,
		c.relkind AS type,
		pg_size_pretty(pg_table_size(c.oid::regclass)) AS size,
		pg_table_size(c.oid::regclass) AS size_in_bytes,
		pg_size_pretty(pg_indexes_size(c.oid::regclass)) AS idxs_size,
		pg_size_pretty(pg_total_relation_size(c.oid::regclass)) AS total,
		pg_table_size(c.oid::regclass) AS size_raw,
		pg_indexes_size(c.oid::regclass) AS idxsize_raw,
		pg_total_relation_size(c.oid::regclass) AS total_raw,
		c.n_live_tup,
		c.n_dead_tup,
		c.n_tup_ins,
		c.n_tup_upd,
		c.n_tup_hot_upd,
		c.oid AS rel_oid,
		n.oid AS schema_oid,
		c.relkind
	FROM pg_class_prep c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE (n.nspname <> ALL (ARRAY['pg_catalog'::name, 'information_schema'::name])) 
		AND n.nspname !~ '^pg_toast'::text AND (c.relkind = ANY (ARRAY['r'::"char", 'i'::"char"]))
	ORDER BY pg_total_relation_size(c.oid::regclass) DESC
) T
LIMIT 30;