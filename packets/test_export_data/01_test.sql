DROP TABLE IF EXISTS public.test_tbl_export_1;
DROP TABLE IF EXISTS public.test_tbl_export_2;

CREATE TABLE public.test_tbl_export_1
(
    id serial,
    fld_1 bigint,
    fld_2 text
);

INSERT INTO public.test_tbl_export_1(fld_1, fld_2)
    SELECT T.v, 'text_' || T.v from(SELECT generate_series(1, 200000) as v) T;

CREATE TABLE public.test_tbl_export_2
(
    id serial,
    fld_1 bigint,
    fld_2 text
);

INSERT INTO public.test_tbl_export_2(fld_1, fld_2)
    SELECT T.v, 'text_' || T.v from(SELECT generate_series(1, 200000) as v) T;
