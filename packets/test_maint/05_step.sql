CREATE INDEX CONCURRENTLY dbc_packets_dt_test_idx
    ON public.dbc_packets USING btree
    (dt);