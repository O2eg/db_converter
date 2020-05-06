------------------
-- tx start
do $$ 
begin
	raise exception 'some exception';
end$$;
-- tx end
------------------