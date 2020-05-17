------------------
-- tx start
do $$
begin
	if exists( SELECT 1
	FROM information_schema.columns
	WHERE table_schema = 'public' and table_name = 'test_tbl' and column_name = 'id_new' ) = false
	then
		ALTER TABLE public.test_tbl ADD COLUMN id_new bigint;

		--------------------------------------------------
		CREATE FUNCTION public.dbc_ins_tg_test_tbl_func()
			RETURNS trigger
			LANGUAGE 'plpgsql'
			COST 100
		AS $BODY$
		  BEGIN
			NEW.id_new := NEW.id;
			return NEW;
		  END;
		$BODY$;

		CREATE TRIGGER test_tbl_tg
			BEFORE INSERT
			ON public.test_tbl
			FOR EACH ROW
			EXECUTE PROCEDURE public.dbc_ins_tg_test_tbl_func();
		--------------------------------------------------
		ALTER TABLE public.test_tbl SET (autovacuum_enabled = false);
	end if;
end$$;
-- tx end
------------------