SELECT
    n.nspname,
    c.relname,
    age(c.relfrozenxid) as relfrozenxid_age,
    mxid_age(c.relminmxid) as relminmxid_age
FROM pg_class c
JOIN pg_namespace n on c.relnamespace = n.oid
WHERE c.relkind IN ('r', 'm', 't') and
	(age(c.relfrozenxid) > 1300000000 or mxid_age(c.relminmxid) > 1400000000)
ORDER BY age(c.relfrozenxid) desc
LIMIT 10;