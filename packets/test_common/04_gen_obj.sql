------------------
-- this generator returns list of tables
select 'analyze pg_amop' as maint, 'tbl_a'
UNION
select 'analyze pg_am', 'tbl_b'
UNION
select null, 'tbl_c'
------------------