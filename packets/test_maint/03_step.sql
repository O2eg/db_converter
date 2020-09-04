drop index if exists dbc.dbc_packets_dt_test_idx;

CREATE INDEX dbc_packets_dt_test_idx
    ON dbc.dbc_packets USING btree
    (dt);