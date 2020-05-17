CREATE UNIQUE INDEX CONCURRENTLY test_tbl_id_new_idx
    ON public.test_tbl USING btree (id_new);