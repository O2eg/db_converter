ALTER TABLE public.test_tbl ADD CONSTRAINT test_tbl_pkey_new
    PRIMARY KEY USING INDEX test_tbl_id_new_idx;

ALTER TABLE public.test_tbl RENAME id TO id_old;
ALTER TABLE public.test_tbl RENAME id_new TO id;
ALTER TABLE public.test_tbl
   ALTER COLUMN id set default nextval('test_tbl_id_seq');

ALTER SEQUENCE test_tbl_id_seq OWNED BY public.test_tbl.id;
ALTER TABLE public.test_tbl DROP COLUMN id_old;

DROP TRIGGER test_tbl_tg
    ON public.test_tbl;

DROP FUNCTION public.dbc_ins_tg_test_tbl_func();
ALTER TABLE public.test_tbl SET (autovacuum_enabled = true);
