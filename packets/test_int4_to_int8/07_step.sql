INSERT INTO public.test_tbl(fld_1, fld_2)
	 SELECT T.v, 'text_' || T.v from(SELECT generate_series(4000000000,4000000010) as v) T;