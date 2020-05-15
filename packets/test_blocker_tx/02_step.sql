do $$ 
begin
perform * from public.test_blocker_tx_tbl;
perform pg_sleep(10);
end$$