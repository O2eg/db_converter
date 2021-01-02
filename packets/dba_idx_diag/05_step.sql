-- Issue: there is an unused index
-- Solution: remove unused index
select
	pg_size_pretty(pg_relation_size(cr.oid)) as tbl_size,
	pg_size_pretty(pg_relation_size(ci.oid)) as idx_size,
	n.nspname,
	cr.relname as tbl_name,
	ci.relname as idx_name,
	sat.idx_scan as idx_scan_total,
	sai.idx_scan,
	round(sai.idx_scan::decimal/sat.idx_scan * 100, 3) as idx_scan_ratio
from pg_index i
join pg_class ci on ci.oid = i.indexrelid and ci.relkind = 'i'
join pg_class cr on cr.oid = i.indrelid and cr.relkind = 'r'
join pg_namespace n on n.oid = ci.relnamespace
	and nspname not in ('pg_catalog', 'pg_toast', 'information_schema')
join pg_stat_all_indexes sai on sai.indexrelid = i.indexrelid and sai.relid = i.indrelid
join pg_stat_all_tables sat on sat.relid = cr.oid
where
	cr.relpages > 100 and
	ci.relpages > 0 and
	sat.idx_scan > 0 and
	(
		sat.idx_scan > 1000 or sat.seq_scan > 1000 -- detect real workload
	) and
	sai.idx_scan::decimal/sat.idx_scan < 0.01
order by idx_scan_ratio, idx_name
limit 100