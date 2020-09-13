from psc.psccommon.psc_common import *


class ActionTracker:

    @staticmethod
    def cleanup(db_conn, schema_location):
        db_conn.execute("""
            DROP TABLE IF EXISTS {0}.dbc_packets CASCADE;
            DROP TABLE IF EXISTS {0}.dbc_steps CASCADE;
            DROP TABLE IF EXISTS {0}.dbc_actions CASCADE;
            DROP TABLE IF EXISTS {0}.dbc_locks CASCADE;
        """.format(schema_location)
        )

    @staticmethod
    def init_tbls(db_conn, schema_location):
        db_conn.execute("""
        DO $$
            begin
            IF (
                 SELECT count(1)
                 FROM   pg_class c
                 JOIN   pg_namespace n ON n.oid = c.relnamespace
                 WHERE  c.relname in ('dbc_packets', 'dbc_steps', 'dbc_actions', 'dbc_locks')
                    AND c.relkind = 'r'
                    AND n.nspname = '{0}'
            ) != 4
            THEN
                SET search_path = '{0}';
                CREATE SCHEMA IF NOT EXISTS {0};

                DROP TABLE IF EXISTS dbc_packets CASCADE;
                DROP TABLE IF EXISTS dbc_steps CASCADE;
                DROP TABLE IF EXISTS dbc_actions CASCADE;
                DROP TABLE IF EXISTS dbc_locks CASCADE;
    
                CREATE TABLE dbc_packets
                (
                    id serial,
                    name character varying(128) not null,
                    status character varying(10) default 'started',
                    dt timestamp with time zone default now(),
                    packet_hash character varying(32) not null,
                    meta_data jsonb not null,
                    CONSTRAINT dbc_packets_pkey PRIMARY KEY (id),
                    CONSTRAINT packet_status CHECK (status in ('done', 'started', 'exception'))
                );
    
                CREATE TABLE dbc_steps
                (
                    id serial,
                    name character varying(128) not null,
                    packet_id integer,
                    status character varying(10) default 'started',
                    dt timestamp with time zone default now(),
                    exception_descr text,
                    CONSTRAINT dbc_steps_pkey PRIMARY KEY (id),
                    CONSTRAINT fk_dbc_packets_id FOREIGN KEY (packet_id)
                        REFERENCES dbc_packets (id),
                    CONSTRAINT step_status CHECK (status in ('done', 'started', 'exception'))
                );
    
                CREATE TABLE dbc_actions
                (
                    dt timestamp with time zone default now(),
                    packet_id integer,
                    step_id integer,
                    step_hash character varying(32) not null,
                    CONSTRAINT dbc_actions_uniq UNIQUE (packet_id, step_id, step_hash),
                    CONSTRAINT fk_dbc_packets_id FOREIGN KEY (packet_id)
                        REFERENCES dbc_packets (id),
                    CONSTRAINT fk_dbc_steps_id FOREIGN KEY (step_id)
                        REFERENCES dbc_steps (id)
                );
                
                CREATE TABLE dbc_locks
                (
                    id serial,
                    name character varying(128) not null,
                    locked boolean not null default true,
                    dt timestamp with time zone default now(),
                    CONSTRAINT dbc_locks_pkey PRIMARY KEY (id)
                );
    
                CREATE INDEX dbc_actions_dt_idx
                    ON dbc_actions USING btree (dt);
                CREATE INDEX dbc_actions_step_hash_idx
                    ON dbc_actions USING btree (step_hash);
                CREATE INDEX dbc_packets_meta_data_idx
                    ON dbc_packets USING GIN (meta_data);
                CREATE UNIQUE INDEX dbc_packets_name_idx
                    ON dbc_packets USING btree (name);
                CREATE UNIQUE INDEX dbc_locks_name_idx
                    ON dbc_locks USING btree (name);
            END IF;
            end$$;
        """.format(schema_location)
        )

    @staticmethod
    def is_action_exists(db_conn, schema_location, packet, step, step_hash):
        return get_scalar(
            db_conn,
            """
                SELECT EXISTS(
                    SELECT 1
                    FROM %s.dbc_actions a
                    INNER JOIN %s.dbc_packets p ON a.packet_id = p.id
                    INNER JOIN %s.dbc_steps s ON a.step_id = s.id
                    WHERE p.name = '%s'
                      AND s.name = '%s'
                      AND a.step_hash = '%s'
                )
            """ % (schema_location, schema_location, schema_location, packet, step, step_hash)
        )

    @staticmethod
    def is_packet_locked(db_conn, schema_location, packet):
        return get_scalar(
            db_conn,
            """
                SELECT EXISTS(
                    SELECT 1
                    FROM %s.dbc_locks p
                    WHERE p.name = '%s' and locked = true
                )
            """ % (schema_location, packet)
        )

    @staticmethod
    def set_packet_lock(db_conn, schema_location, packet):
        db_conn.execute(
            """
            DO $$
            declare
                packet_name text = '%s';
            begin
                SET search_path = '%s';
                IF EXISTS(
                    SELECT 1
                    FROM dbc_locks p
                    WHERE p.name = packet_name
                ) THEN
                    UPDATE dbc_locks
                        SET locked = true
                        WHERE name = packet_name;
                ELSE
                    INSERT INTO dbc_locks(name, locked)
                       VALUES (packet_name, true);
                END IF;
            end$$;
            """ % (packet, schema_location)
        )

    @staticmethod
    def set_packet_unlock(db_conn, schema_location, packet):
        db_conn.execute(
            """
                UPDATE %s.dbc_locks
                    SET locked = false
                    WHERE name = '%s';
            """ % (schema_location, packet)
        )

    @staticmethod
    def begin_action(db_conn, schema_location, packet_name, packet_hash, step_name, meta_data):
        # "action" will be inserted into "dbc_actions" only in case of successful execution,
        # but before execution of "action", it is necessary to set status of "step" = "started"
        db_conn.execute("""
        DO $$
            declare
                packet_name text = '%s';
                step_name text = '%s';
                packet_id_v integer = NULL;
                step_id_v integer = NULL;
            begin
                SET search_path = '%s';
                SELECT id into packet_id_v
                FROM   dbc_packets
                WHERE  name = packet_name;
    
                IF packet_id_v IS NULL THEN
                    INSERT INTO dbc_packets(name, status, packet_hash, meta_data)
                       VALUES (packet_name, 'started', '%s', '%s') RETURNING id INTO packet_id_v;
                END IF;
    
                SELECT id into step_id_v
                FROM   dbc_steps
                WHERE  name = step_name and packet_id = packet_id_v;
    
                IF step_id_v IS NULL THEN
                    INSERT INTO dbc_steps(name, packet_id, status)
                       VALUES (step_name, packet_id_v, 'started');
                END IF;
            end$$;
        """ % (packet_name, step_name, schema_location, packet_hash, meta_data)
        )

    @staticmethod
    def apply_action(db_conn, schema_location, packet_name, step_name, step_hash):
        db_conn.execute("""
        DO $$
            declare
                packet_name text = '%s';
                step_name text = '%s';
                packet_id_v integer = NULL;
                step_id_v integer = NULL;
            begin
                SET search_path = '%s';
                SELECT id into packet_id_v  -- id already exist
                FROM   dbc_packets
                WHERE  name = packet_name;
    
                SELECT id into step_id_v    -- id already exist
                FROM   dbc_steps
                WHERE  name = step_name and packet_id = packet_id_v;
    
                INSERT INTO dbc_actions(packet_id, step_id, step_hash)
                        VALUES (packet_id_v, step_id_v, '%s');
            end$$;
        """ % (packet_name, step_name, schema_location, step_hash)
        )

    @staticmethod
    def insert_step(db_conn, schema_location, packet, step):
        db_conn.execute("""
        DO $$
            declare
                packet_name text = '%s';
                step_name text = '%s';
                packet_id_v integer = NULL;
                step_id_v integer = NULL;
            begin
                SET search_path = '%s';
                SELECT id into packet_id_v
                FROM   dbc_packets
                WHERE  name = packet_name;
    
                IF packet_id_v IS NULL THEN
                    INSERT INTO dbc_packets(name, status)
                       VALUES (packet_name, 'started') RETURNING id INTO packet_id_v;
                END IF;
    
                SELECT id into step_id_v
                FROM   dbc_steps
                WHERE  name = step_name and packet_id = packet_id_v;
    
                IF step_id_v IS NULL THEN
                    INSERT INTO dbc_steps(name, packet_id, status)
                       VALUES (step_name, packet_id_v, 'started');
                END IF;
            end$$;
        """ % (packet, step, schema_location)
        )

    @staticmethod
    def wipe_packet(db_conn, schema_location, packet):
        is_data_exists = get_scalar(
            db_conn,
            """
                SELECT EXISTS(
                    SELECT 1
                    FROM %s.dbc_packets p
                    WHERE p.name = '%s'
                )
            """ % (schema_location, packet)
        )
        db_conn.execute("""
            SET search_path = '%s';

            DELETE FROM dbc_actions
            WHERE (packet_id, step_id) IN
                (
                    SELECT
                        a.packet_id,
                        a.step_id
                    FROM dbc_actions a
                    JOIN dbc_steps s ON a.packet_id = s.packet_id
                    JOIN dbc_packets p ON p.id = s.packet_id
                    WHERE p.name = '%s'
                 );
    
            DELETE FROM dbc_steps WHERE packet_id IN
            (
                SELECT p.id
                FROM dbc_packets p
                WHERE p.name = '%s'
            );
    
            DELETE FROM dbc_packets WHERE name = '%s';
        """ % (schema_location, packet, packet, packet)
        )
        return is_data_exists

    @staticmethod
    def set_step_status(db_conn, schema_location, packet_name, step, result):
        db_conn.execute("""
            SET search_path = '%s';

            UPDATE dbc_steps
            SET status = '%s', exception_descr = null
            WHERE id = (
                SELECT s.id from dbc_packets p
                JOIN dbc_steps s on p.id = s.packet_id
                WHERE p.name = '%s' AND s.name = '%s'
            )
        """ % (schema_location, result, packet_name, step)
        )

    @staticmethod
    def set_step_exception_status(db_conn, schema_location, packet_name, step, exception_descr):
        ActionTracker.insert_step(db_conn, schema_location, packet_name, step)
        pquery = db_conn.prepare("""
            UPDATE {0}.dbc_steps
            SET status = 'exception', exception_descr = $1
            WHERE id = (
                SELECT s.id from {0}.dbc_packets p
                JOIN {0}.dbc_steps s on p.id = s.packet_id
                WHERE p.name = $2 AND s.name = $3
            )""".format(schema_location)
        )
        with db_conn.xact(): pquery(exception_descr, packet_name, step)

    @staticmethod
    def set_packet_status(db_conn, schema_location, packet_name, result):
        db_conn.execute("""
                UPDATE %s.dbc_packets
                SET status = '%s'
                WHERE name = '%s'
            """ % (schema_location, result, packet_name)
        )

    @staticmethod
    def get_packet_status(db_conn, schema_location, packet_name):
        res_status = {}
        for rec in get_resultset(db_conn, """
                SELECT p.status, s.exception_descr, s.dt, p.packet_hash
                FROM %s.dbc_packets p
                JOIN %s.dbc_steps s on p.id = s.packet_id
                WHERE p.name = '%s'
                ORDER BY s.dt desc
                LIMIT 1
            """ % (schema_location, schema_location, packet_name)
        ):
            res_status["status"] = rec[0]
            res_status["exception_descr"] = rec[1]
            res_status["exception_dt"] = rec[2]
            res_status["hash"] = rec[3]

        return res_status
