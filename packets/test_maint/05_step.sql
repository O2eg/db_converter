CREATE INDEX CONCURRENTLY dbc_packets_dt_test_idx
    ON dbc.dbc_packets USING btree
    (dt);