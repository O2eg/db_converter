------------------
-- tx start
do $$ 
begin
	raise notice 'Pre-select notice 1...';
end$$;

select 1 as a, 2 as b, 3 as c
union
select 4, 5, 6;
-- tx end
------------------