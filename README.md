# db_converter

[![Build Status](https://travis-ci.com/masterlee998/db_converter.svg?branch=master)](https://travis-ci.com/masterlee998/db_converter)

db_converter is an open-source database migration tool for PostgreSQL designed for high loaded installations.

# How to run:

```bash
nohup python38 db_converter.py \
	--packet-name=my_packet \
	--db-name=ALL
    > /dev/null 2>&1 &

# run all tests
python38 tests/test_packets.py -v
# run specific test
python38 tests/test_packets.py -v TestDBCLock
```

# Dependencies and installation

Python 3.x with modules: sqlparse, requests, pyzipper

```bash
yum install -y python38
# if pip not installed
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.8 get-pip.py
pip3.8 install sqlparse
pip3.8 install requests
pip3.8 install pyzipper
```


# Introduction

The basic goal of `db_converter` is to simplify the database conversion (migration) process as much as possible, while maintaining flexibility and functionality.

Tasks that can be solved using `db_converter`:

* Transactional modification of data of any volume
* Database structure changing
* Database versioning
* System and application notifications via `mattermost` (or any other messenger)
* Database maintenance (deleting old data, creating triggers on partitions, etc.)
* Parallel processing of several databases
* Export data in `csv` format into encrypted archive

## Terminology

**Packet** - is a package of changes (a directory with sql files) that apply to the specified database. Packet contains `meta_data.json` (an optional file with meta information describing the package) and several sql files in `XX_step.sql` format.

**Step** - is a sql file, the contents of which are executed in one transaction, and containing the following types of commands:

* DDL (Data Definition Language) - CREATE, DROP, ALTER, TRUNCATE, COMMENT, RENAME
* DML (Data Manipulation Language) - SELECT, INSERT, UPDATE, DELETE
* DCL (Data Control Language) - GRAND, REVOKE

**Action** - is a transaction formed on the basis of `step`. If `step` does not have a `generator`, then it creates one `action`. If `step` has a `generator`, then several transactions are generated.

**Generator** - is a sql file associated with some `step` by index number. If there is a `generator`, `step` contains placeholders for substituting the values ​​returned by the `generator` (for more details see the "Generators and Placeholders" section).

**Conversion** (migration, deployment) - is the transformation of the database structure according to the specified package of changes.

<p align="center">
  <img src="doc/dbc_common_flow.png">
</p>


When executing the `Packet`, sql files are applied to the specified database sequentially in accordance with the index.
