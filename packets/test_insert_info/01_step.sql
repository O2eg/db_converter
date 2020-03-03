ALTER TABLE dbc_packets SET (autovacuum_enabled = true);
do $$
begin
	raise notice 'some notice';
end$$;
INSERT INTO public.dbc_packets(
	name, packet_hash, meta_data)
	VALUES ('from_test_insert_info', 'acb', '{}');
INSERT INTO public.dbc_packets(
	name, packet_hash, meta_data)
	VALUES ('from_test_insert_info', 'acb', '{}');
UPDATE public.dbc_packets set packet_hash = 'bca';
select id, name, packet_hash from public.dbc_packets where packet_hash = 'bca' limit 10;