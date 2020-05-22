SELECT
    pid, client_addr,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) as pending_wal,
    pg_size_pretty(pg_wal_lsn_diff(sent_lsn, write_lsn)) as write,
    pg_size_pretty(pg_wal_lsn_diff(write_lsn, flush_lsn)) as flush,
    pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn)) as replay,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)) as total_lag
FROM pg_stat_replication;