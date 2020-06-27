set search_path = 'schema_1', 'public';

INSERT INTO schema_1.test_tbl(fld_1, fld_2)
    SELECT T.v, 'text_' || T.v from(SELECT generate_series(1, 3) as v) T;

do $$
begin
	if (select count(1) from schema_1.test_tbl where id > 100) <> 3 then
		raise exception 'test_dba_clone_schema failed';
	end if;
end$$;
