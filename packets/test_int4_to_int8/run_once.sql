DROP TABLE IF EXISTS public.test_tbl;
DROP FUNCTION IF EXISTS public.dbc_ins_tg_test_tbl_func();

CREATE TABLE public.test_tbl
(
    id serial,
    fld_1 bigint,
    fld_2 text
);

INSERT INTO public.test_tbl(fld_1, fld_2)
    SELECT T.v, 'text_' || T.v from(SELECT generate_series(1, 200010) as v) T;
