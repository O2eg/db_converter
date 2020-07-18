------------------
-- tx start
SELECT now();

CREATE TABLE test_tbl
(
    id serial
);

SELECT version();
-- tx end
------------------