#!/bin/bash
set -e

sed -i  '/listen_addresses/s/^#//g' /etc/postgresql/${PG_VERSION}/main/postgresql.conf
sed -ie "s/^listen_addresses.*/listen_addresses = '127.0.0.1'/" /etc/postgresql/${PG_VERSION}/main/postgresql.conf
sed -i -e '/local.*peer/s/postgres/all/' -e 's/peer\|md5/trust/g' /etc/postgresql/${PG_VERSION}/main/pg_hba.conf

pg_ctlcluster ${PG_VERSION} main restart

psql -c "ALTER USER postgres WITH PASSWORD 'YmTLbLTLxF'" -U postgres -h 127.0.0.1
psql -c "CREATE USER dbc_user WITH PASSWORD 'DYy5RexxFZ' createdb" -U postgres -h 127.0.0.1
psql -c "CREATE DATABASE test_db
    WITH 
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    template = template0
    CONNECTION LIMIT = -1" -d postgres -U dbc_user -h 127.0.0.1

ln -s /usr/share/db_converter/db_converter.py /usr/bin/db_converter.py

cat > /usr/bin/dbc << EOL
#!/bin/bash
python3 /usr/share/db_converter/db_converter.py \$@
EOL

chmod +x /usr/bin/dbc

cd /usr/share/db_converter

mv conf/db_converter.conf.example conf/db_converter.conf
# set connection credentials to the database
test_conn='pq:\/\/dbc_user:DYy5RexxFZ@127.0.0.1:5432\/test_db'
sed -ie "s/^test_db_1.*/test_db = $test_conn/" conf/db_converter.conf

echo '[ ! -z "$TERM" -a -r /etc/motd ] && cat /etc/motd' >> /etc/bash.bashrc

trap : TERM INT; sleep infinity & wait