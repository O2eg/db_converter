------------------
-- tx start
	UPDATE public.test_tbl a SET id_new = t.id
	FROM ( select s.id from public.test_tbl s where s.id >= GEN_OBJ_FLD_1 and s.id <= GEN_OBJ_FLD_2 ) AS t
	WHERE a.id = t.id;
-- tx end
------------------