do $$
declare
  val bigint;
  ratio numeric;
begin
      -- schema_name	- GEN_OBJ_FLD_1
      -- table_name		- GEN_OBJ_FLD_2
      -- typname		- GEN_OBJ_FLD_3
      -- attname		- GEN_OBJ_FLD_4
      -- seq			- GEN_OBJ_FLD_5

    if 'GEN_OBJ_FLD_5' = 'None' then
        execute format('select max(%I) from %I.%I;', 'GEN_OBJ_FLD_4', 'GEN_OBJ_FLD_1', 'GEN_OBJ_FLD_2') into val;
    else
        execute format('SELECT last_value FROM %s', 'GEN_OBJ_FLD_5') into val;
    end if;
    if 'GEN_OBJ_FLD_3' = 'int4' then
      ratio := (val::numeric / 2^31)::numeric;
    elsif 'GEN_OBJ_FLD_3' = 'int2' then
      ratio := (val::numeric / 2^15)::numeric;
    end if;
    if ratio > 0.7 then -- report only if > 50% of capacity is reached
		raise notice '%', format('tbl = %s pk = %s type = %s current_value = %s capacity = %s',
			coalesce(nullif(quote_ident('GEN_OBJ_FLD_1'), 'public') || '.', '') || quote_ident('GEN_OBJ_FLD_2'),
			'GEN_OBJ_FLD_4',
			'GEN_OBJ_FLD_3',
			val,
			round(100 * ratio, 2)
		);
    end if;
end;
$$