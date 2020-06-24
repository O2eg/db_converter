DROP TABLE IF EXISTS public.test_tbl_import;

CREATE TABLE public.test_tbl_import
(
    id serial,
    fld_1 text
);

INSERT INTO public.test_tbl_import(fld_1)
    SELECT 'text_' || T.v from(SELECT generate_series(1, 20) as v) T;
