------------------
select null as maint, case when not exists(select 1 from pg_database where datname = 'test_dbc_01')
	then 'CREATE DATABASE test_dbc_01
    WITH 
    OWNER = postgres
    ENCODING = ''UTF8''
    LC_COLLATE = ''en_US.UTF-8''
    LC_CTYPE = ''en_US.UTF-8''
    TABLESPACE = pg_default
	template=template0
    CONNECTION LIMIT = -1;'
	else '' end
union
select null, case when not exists(select 1 from pg_database where datname = 'test_dbc_02')
	then 'CREATE DATABASE test_dbc_02
    WITH 
    OWNER = postgres
    ENCODING = ''UTF8''
    LC_COLLATE = ''en_US.UTF-8''
    LC_CTYPE = ''en_US.UTF-8''
    TABLESPACE = pg_default
	template=template0
    CONNECTION LIMIT = -1;'
	else '' end
------------------