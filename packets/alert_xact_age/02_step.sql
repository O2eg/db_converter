SELECT
	datname,
	age(datfrozenxid) as datfrozenxid_age,
	mxid_age(datminmxid) as datminmxid_age
FROM pg_database
WHERE (age(datfrozenxid) > 1300000000 or mxid_age(datminmxid) > 1400000000)
ORDER BY age(datfrozenxid) DESC;