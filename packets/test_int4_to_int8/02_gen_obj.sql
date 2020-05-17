------------------
with pk_intervals as (
	 select 0 as point_a,
		(select max( id ) from public.test_tbl) as point_b
)
	select
		(case when T.id % 5 = 1 then 'vacuum analyze public.test_tbl' end) as maint,	-- "maint" is system field
		T.a,	-- GEN_OBJ_FLD_1
		T.b		-- GEN_OBJ_FLD_2
	from (
		with grid_a as (
			select T.point_a as point_a, row_number() OVER () as id
			from (
				SELECT generate_series((select point_a from pk_intervals), (select  point_b from pk_intervals) + 10000, 10000) as point_a
				ORDER BY 1
			) T
		),
		grid_b as (
			select T.point_b as point_b, row_number() OVER () as id
			from (
				SELECT generate_series((select point_a from pk_intervals) + 100000, (select point_b from pk_intervals) + 10000, 10000) as point_b
				ORDER BY 1
			) T
		)
		SELECT A.point_a as a, B.point_b as b, A.id from grid_a A
		join grid_b B on A.id = B.id
	) T;
------------------