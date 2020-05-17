------------------
-- tx start
do $$
begin
    if ( SELECT is_nullable
    FROM information_schema.columns 
    WHERE table_schema = 'public' and table_name = 'test_tbl' and column_name = 'id_new' ) = 'YES'
    then
        ALTER TABLE public.test_tbl ADD CONSTRAINT id_new_not_null CHECK (id_new IS NOT NULL) NOT VALID;
        ALTER TABLE public.test_tbl VALIDATE CONSTRAINT id_new_not_null;
    end if;
end$$;
-- tx end
------------------