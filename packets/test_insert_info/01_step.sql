DELETE from dbc.dbc_packets where name = 'from_test_insert_info';
ALTER TABLE dbc.dbc_packets SET (autovacuum_enabled = true);
do $$
begin
	raise notice 'some notice';
end$$;
INSERT INTO dbc.dbc_packets(
	name, packet_hash, meta_data)
	VALUES ('from_test_insert_info', 'acb', '{}');
UPDATE dbc.dbc_packets set packet_hash = 'bca' where packet_hash = 'abc';
select id, name, packet_hash from dbc.dbc_packets where packet_hash = 'bca' limit 10;