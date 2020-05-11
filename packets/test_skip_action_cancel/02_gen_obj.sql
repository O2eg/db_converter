------------------
select null as maint, 'select 2.1', 1 as num
UNION
select null, 'select pg_sleep(10000)', 2
UNION
select null, 'select 2.3', 3
order by num
------------------