drop index if exists public.dbc_packets_dt_test_idx;

CREATE INDEX dbc_packets_dt_test_idx
    ON public.dbc_packets USING btree
    (dt);