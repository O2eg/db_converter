------------------
do $$
declare
	nsps text[] = '{"nsp_a", "nsp_b", "nsp_c"}';
	tbls text[] = '{"tbl_a", "tbl_b", "tbl_c"}';
	nsp_i text;
	tbl_i text;
begin
	for nsp_i in (select unnest(nsps)) loop
		execute format('DROP schema if exists %s cascade', nsp_i);
		execute format('CREATE schema %s', nsp_i);
	end loop;

	for nsp_i in (select unnest(nsps)) loop
		for tbl_i in (select unnest(tbls)) loop
			execute format('CREATE table %s.%s(fld integer)', nsp_i, tbl_i);
			execute format('INSERT INTO %s.%s(fld) SELECT generate_series(1, 30)', nsp_i, tbl_i);
		end loop;
	end loop;
end$$;
------------------