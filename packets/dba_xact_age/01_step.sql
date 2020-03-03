SELECT n.nspname, c.relname, greatest(age(c.relfrozenxid),age(t.relfrozenxid)) as age
FROM pg_class c
JOIN pg_namespace n on c.relnamespace = n.oid
LEFT JOIN pg_class t ON c.reltoastrelid = t.oid
WHERE c.relkind IN ('r', 'm') and greatest(age(c.relfrozenxid),age(t.relfrozenxid)) > 1200000000
ORDER BY greatest(age(c.relfrozenxid),age(t.relfrozenxid)) desc
LIMIT 10;

SELECT datname, age(datfrozenxid) FROM pg_database
ORDER BY age(datfrozenxid) desc;