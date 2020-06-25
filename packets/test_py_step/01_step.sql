DROP TABLE IF EXISTS public.test_tbl_import;

CREATE TABLE public.test_tbl_import
(
    id serial,
	dir text,
	fname text,
    content text
);
