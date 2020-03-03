------------------
select null as maint, 'nsp_a'
UNION
select 'analyze pg_amop', 'nsp_b'
UNION
select null, 'nsp_c'
------------------