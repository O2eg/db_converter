import unittest
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from db_converter import *
import psc.postgresql as postgresql
from actiontracker import ActionTracker
import pyzipper
import difflib
from unittest import mock
from test_psc import *


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


def mocked_requests_post(*args, **kwargs):
    class Response:
        status_code = 200
    return Response()


class CommonVars:
    conf_file = 'db_converter_test.conf'
    packets_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        "packets"
    )
    # databases from conf_file
    pg_db = 'pg_db'
    test_dbc_01 = 'test_dbc_01'
    test_dbc_02 = 'test_dbc_02'
    test_dbc_packets = 'test_dbc_packets'   # this database is recreated for each packet unit test


class TestDBCPackets(unittest.TestCase, CommonVars):
    wipes = []
    runs = []

    def setUp(self):
        del self.wipes[:]
        del self.runs[:]

        packets = [
            f for f in os.listdir(self.packets_dir)
            if f.startswith('test_') and os.path.isdir(os.path.join(self.packets_dir, f)) and f not in [
                'test_sleep_sigint',
                'test_skip_step_cancel',
                'test_skip_action_cancel',
                'test_prepare_dbs',
                'test_blocker_tx',
                'test_wait_tx',
                'test_int4_to_int8',
                'test_export_data',
                'test_py_step',
                'test_override_conf_param',
                'test_placeholders',
                'test_get_version',
                'test_dba_idx_diag'
            ]
        ]
        packets.sort()
        for f_name in packets:
            args = dict(
                packet_name=f_name,
                db_name='test_dbc_01',
                template=None,
                list=None,
                status=None,
                stop=None,
                wipe=True,
                seq=False,
                force=False,
                unlock=False
            )
            self.wipes.append(Struct(**args))

            args = dict(
                packet_name=f_name,
                db_name='test_dbc_01',
                template=None,
                list=None,
                status=None,
                stop=None,
                wipe=None,
                seq=False,
                force=False,
                unlock=False
            )

            self.runs.append([
                Struct(**args),
                ResultCode.SUCCESS if f_name.find('exception') == -1 else ResultCode.FAIL
            ])

    @mock.patch('matterhook.incoming.requests.post', side_effect=mocked_requests_post)
    def test_packets(self, mocked_requests_post):
        for args in self.wipes:
            res = MainRoutine(args, self.conf_file).run()
            self.assertTrue(
                res.result_code[args.db_name] == ResultCode.NOTHING_TODO or
                res.result_code[args.db_name] == ResultCode.SUCCESS
            )
            self.assertTrue(res.packet_status[args.db_name] == PacketStatus.NEW)

        for args in self.runs:
            print("=================> TestDBCPackets: " + str(args[0].packet_name))
            res = MainRoutine(args[0], self.conf_file).run()
            if args[1] == ResultCode.SUCCESS:
                self.assertTrue(res.result_code[args[0].db_name] == ResultCode.SUCCESS)
                self.assertTrue(res.packet_status[args[0].db_name] == PacketStatus.DONE)
            if args[1] == ResultCode.FAIL:
                self.assertTrue(res.result_code[args[0].db_name] == ResultCode.FAIL)
                self.assertTrue(res.packet_status[args[0].db_name] == PacketStatus.EXCEPTION)


class TestDBCLock(unittest.TestCase, CommonVars):
    wipe_params = None
    run_params = None
    packet_name = 'test_sleep'

    def setUp(self):
        wipe_args = dict(
            packet_name=self.packet_name,
            db_name=self.test_dbc_01,
            template=None,
            list=None,
            status=None,
            stop=None,
            wipe=True,
            seq=False,
            force=False,
            unlock=False
        )
        self.wipe_params = Struct(**wipe_args)

        run_args = dict(
            packet_name=self.packet_name,
            db_name=self.test_dbc_01,
            template=None,
            list=None,
            status=None,
            stop=None,
            wipe=None,
            seq=False,
            force=False,
            unlock=False
        )

        self.run_params = Struct(**run_args)

    @mock.patch('matterhook.incoming.requests.post', side_effect=mocked_requests_post)
    def test_lock(self, mocked_requests_post):
        dbc = MainRoutine(self.wipe_params, self.conf_file)
        res_1 = dbc.run()
        self.assertTrue(res_1.packet_status[self.test_dbc_01] == PacketStatus.NEW)
        self.assertTrue(res_1.result_code[self.test_dbc_01] == ResultCode.NOTHING_TODO)

        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
        ActionTracker.set_packet_lock(db_conn, dbc.sys_conf.schema_location, self.packet_name)

        res_2 = MainRoutine(self.run_params, self.conf_file).run()
        self.assertTrue(res_2.packet_status[self.test_dbc_01] == PacketStatus.STARTED)
        self.assertTrue(res_2.result_code[self.test_dbc_01] == ResultCode.LOCKED)

        ActionTracker.set_packet_unlock(db_conn, dbc.sys_conf.schema_location, self.packet_name)
        db_conn.close()

        res_3 = MainRoutine(self.run_params, self.conf_file).run()
        self.assertTrue(res_3.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res_3.result_code[self.test_dbc_01] == ResultCode.SUCCESS)


class TestDBCLockKey(unittest.TestCase, CommonVars):
    packet_name = 'test_sleep'

    @mock.patch('matterhook.incoming.requests.post', side_effect=mocked_requests_post)
    def test_lock(self, mocked_requests_post):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args(['--packet-name=' + self.packet_name, '--db-name=' + self.test_dbc_01])
        dbc = MainRoutine(args, self.conf_file)

        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
        ActionTracker.set_packet_lock(db_conn, dbc.sys_conf.schema_location, self.packet_name)

        res_1 = dbc.run()
        self.assertTrue(res_1.packet_status[self.test_dbc_01] == PacketStatus.STARTED)
        self.assertTrue(res_1.result_code[self.test_dbc_01] == ResultCode.LOCKED)

        ActionTracker.set_packet_unlock(db_conn, dbc.sys_conf.schema_location, self.packet_name)

        res_2 = MainRoutine(args, self.conf_file).run()
        self.assertTrue(res_2.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res_2.result_code[self.test_dbc_01] == ResultCode.SUCCESS)

        db_conn.close()


class TestDBCSignal(unittest.TestCase, CommonVars):
    packet_name = 'test_sleep_sigint'

    @mock.patch('matterhook.incoming.requests.post', side_effect=mocked_requests_post)
    def test_sigint(self, mocked_requests_post):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args(['--packet-name=' + self.packet_name, '--db-name=' + self.test_dbc_01])

        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
        ActionTracker.set_packet_unlock(db_conn, dbc.sys_conf.schema_location, self.packet_name)
        db_conn.close()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def send_signal():
            time.sleep(5)
            pid = os.getpid()
            os.kill(pid, signal.SIGINT)

        @threaded
        def emulate_signal():       # for Windows
            time.sleep(5)
            main.external_interrupt = True
            time.sleep(3)
            th_db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
            main.terminate_conns(
                th_db_conn, self.test_dbc_01, main.sys_conf.application_name, self.packet_name
            )
            th_db_conn.close()

        if os.name == 'nt':
            main.append_thread(self.test_dbc_01 + '_ext', emulate_signal())
        else:
            main.append_thread(self.test_dbc_01 + '_ext', send_signal())

        res = main.run()

        self.assertTrue(res.packet_status[self.test_dbc_01] == PacketStatus.STARTED)
        self.assertTrue(res.result_code[self.test_dbc_01] == ResultCode.TERMINATE)


class TestDBCUnknownDB(unittest.TestCase, CommonVars):
    packet_name = 'test_sleep_sigint'
    db_names = ['test_dbc_unknown_1', 'test_dbc_unknown_2']

    def test_unknown_db(self):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args(['--packet-name=' + self.packet_name, '--db-name=' + ','.join(self.db_names)])

        main = MainRoutine(args, self.conf_file)
        res = main.run()

        for db in self.db_names:
            self.assertTrue(res.packet_status[db] == PacketStatus.UNKNOWN)
            self.assertTrue(res.result_code[db] == ResultCode.NOTHING_TODO)


class TestDBCConnErr(unittest.TestCase, CommonVars):
    packet_name = 'test_sleep'

    def test_conn_err(self):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args(['--packet-name=' + self.packet_name, '--db-name=' + self.test_dbc_01])

        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
        ActionTracker.init_tbls(db_conn, dbc.sys_conf.schema_location)
        ActionTracker.set_packet_unlock(db_conn, dbc.sys_conf.schema_location, self.packet_name)
        db_conn.close()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def emulate_conn_error():
            time.sleep(2)
            th_db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
            main.terminate_conns(
                th_db_conn, self.test_dbc_01, main.sys_conf.application_name, self.packet_name
            )
            th_db_conn.close()

        main.append_thread(self.test_dbc_01 + '_ext', emulate_conn_error())

        res = main.run()

        self.assertTrue(res.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res.result_code[self.test_dbc_01] == ResultCode.SUCCESS)


class TestDBCSkipStepCancel(unittest.TestCase, CommonVars):
    packet_name = 'test_skip_step_cancel'

    def test_skip_step_cancel(self):
        parser = DBCParams.get_arg_parser()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--wipe'
        ]), self.conf_file).run()

        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--skip-step-cancel'
        ])

        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
        ActionTracker.set_packet_unlock(db_conn, dbc.sys_conf.schema_location, self.packet_name)
        db_conn.close()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def emulate_conn_error():
            time.sleep(3)
            th_db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
            main.terminate_conns(
                th_db_conn, self.test_dbc_01, main.sys_conf.application_name, self.packet_name, terminate=False
            )
            th_db_conn.close()

        main.append_thread(self.test_dbc_01 + '_ext', emulate_conn_error())

        res_2 = main.run()

        self.assertTrue(res_2.packet_status[self.test_dbc_01] == PacketStatus.EXCEPTION)
        self.assertTrue(res_2.result_code[self.test_dbc_01] == ResultCode.FAIL)


class TestDBCSkipActionCancel(unittest.TestCase, CommonVars):
    packet_name = 'test_skip_action_cancel'

    def test_skip_action_cancel(self):
        parser = DBCParams.get_arg_parser()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--wipe'
        ]), self.conf_file).run()

        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--skip-action-cancel'
        ])

        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
        ActionTracker.set_packet_unlock(db_conn, dbc.sys_conf.schema_location, self.packet_name)
        db_conn.close()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def emulate_conn_error():
            time.sleep(5)
            th_db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
            main.terminate_conns(
                th_db_conn, self.test_dbc_01, main.sys_conf.application_name, self.packet_name, terminate=False
            )
            th_db_conn.close()

        main.append_thread(self.test_dbc_01 + '_ext', emulate_conn_error())

        res_2 = main.run()

        self.assertTrue(res_2.packet_status[self.test_dbc_01] == PacketStatus.EXCEPTION)
        self.assertTrue(res_2.result_code[self.test_dbc_01] == ResultCode.FAIL)


class TestDBCPrepareDBs(unittest.TestCase, CommonVars):
    packet_name = 'test_prepare_dbs'

    def test_create_db(self):
        global call_TestDBCPrepareDBs
        if call_TestDBCPrepareDBs:
            return

        parser = DBCParams.get_arg_parser()

        args = parser.parse_args([
            '--packet-name=' + self.packet_name, '--db-name=' + self.pg_db, '--wipe'
        ])
        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.pg_db])
        ActionTracker.cleanup(db_conn, dbc.sys_conf.schema_location)

        db_conn.execute("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
                AND datname in ('test_dbc_01', 'test_dbc_02')
        """)

        db_conn.execute("""DROP DATABASE IF EXISTS test_dbc_01""")
        db_conn.execute("""DROP DATABASE IF EXISTS test_dbc_02""")
        db_conn.close()

        MainRoutine(args, self.conf_file).run()
        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name, '--db-name=' + self.pg_db, '--unlock'
        ]), self.conf_file).run()
        res_1 = MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name, '--db-name=' + self.pg_db
        ]), self.conf_file).run()

        call_TestDBCPrepareDBs = True


class TestDBCBlockerTxTimeout(unittest.TestCase, CommonVars):
    packet_name = 'test_blocker_tx'

    @mock.patch('matterhook.incoming.requests.post', side_effect=mocked_requests_post)
    def test_blocker_tx(self, mocked_requests_post):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01
        ])

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--wipe'
        ]), self.conf_file).run()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def emulate_workload():
            time.sleep(3)
            th_db_conn = postgresql.open(main.sys_conf.dbs_dict[self.test_dbc_01])
            th_db_conn.execute("""vacuum full public.test_blocker_tx_tbl""")
            th_db_conn.close()

        main.append_thread(self.test_dbc_01 + '_ext', emulate_workload())

        res_2 = main.run()

        self.assertTrue(main.lock_observer_blocker_cnt == 1)
        self.assertTrue(res_2.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res_2.result_code[self.test_dbc_01] == ResultCode.SUCCESS)


class TestDBCWaitTxTimeout(unittest.TestCase, CommonVars):
    packet_name = 'test_wait_tx'

    @mock.patch('matterhook.incoming.requests.post', side_effect=mocked_requests_post)
    def test_wait_tx(self, mocked_requests_post):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01
        ])

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--wipe'
        ]), self.conf_file).run()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def emulate_workload():
            th_db_conn = postgresql.open(main.sys_conf.dbs_dict[self.test_dbc_01])
            th_db_conn.execute("""
                do $$
                begin
                perform pg_sleep(3);
                perform * from public.test_wait_tx_tbl;
                perform pg_sleep(10);
                end$$
            """)
            th_db_conn.close()

        main.append_thread(self.test_dbc_01 + '_ext', emulate_workload())

        res_2 = main.run()

        self.assertTrue(main.lock_observer_wait_cnt == 1)
        self.assertTrue(res_2.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res_2.result_code[self.test_dbc_01] == ResultCode.SUCCESS)

        res = MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--status'
        ]), self.conf_file).run()

        self.assertTrue(res.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res.result_code[self.test_dbc_01] == ResultCode.SUCCESS)


class TestDBCInt4ToInt8(unittest.TestCase, CommonVars):
    packet_name = 'test_int4_to_int8'

    def test_int4_to_int8(self):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=test_dbc_*',
        ])

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=test_dbc_*',
            '--wipe'
        ]), self.conf_file).run()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def emulate_workload(db_name):
            time.sleep(1)
            th_db_conn = postgresql.open(db_name)
            i = 1
            try:
                for i in range(1, 500):
                    th_db_conn.execute("""
                        INSERT INTO public.test_tbl(fld_1, fld_2)
                            VALUES (%d, 'emulate_workload_%d');
                    """ % (i, i))
                    time.sleep(0.01)
                    i += 1
            except:
                return
            th_db_conn.close()
            print('================> thread emulate_workload finished for DB %s' % db_name)

        main.append_thread('test_dbc_01_ext_th', emulate_workload(main.sys_conf.dbs_dict[self.test_dbc_01]))
        main.append_thread('test_dbc_02_ext_th', emulate_workload(main.sys_conf.dbs_dict[self.test_dbc_02]))

        res = main.run()

        self.assertTrue(res.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res.result_code[self.test_dbc_01] == ResultCode.SUCCESS)
        self.assertTrue(res.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res.result_code[self.test_dbc_01] == ResultCode.SUCCESS)


class TestDBCAlertAndDBAPackets(unittest.TestCase, CommonVars):
    runs = []

    def setUp(self):
        for f_name in [
            f for f in os.listdir(self.packets_dir)
            if (f.startswith('alert_') or f.startswith('dba_')) and os.path.isdir(os.path.join(self.packets_dir, f))
        ]:
            args = dict(
                packet_name=f_name,
                db_name=self.test_dbc_01,
                template=None,
                list=None,
                status=None,
                stop=None,
                wipe=None,
                seq=False,
                force=False,
                unlock=False
            )
            self.runs.append(Struct(**args))

    @mock.patch('matterhook.incoming.requests.post', side_effect=mocked_requests_post)
    def test_packets(self, mocked_requests_post):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args(['--packet-name=dba_get_version', '--db-name=' + self.test_dbc_01])
        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.test_dbc_01])
        ActionTracker.init_tbls(db_conn, dbc.sys_conf.schema_location)

        for args in self.runs:
            ActionTracker.set_packet_unlock(db_conn, dbc.sys_conf.schema_location, args.packet_name)
            res = MainRoutine(args, self.conf_file).run()
            self.assertTrue(res.result_code[args.db_name] == ResultCode.SUCCESS)
            self.assertTrue(res.packet_status[args.db_name] == PacketStatus.DONE)

        db_conn.close()


class TestDBCExportData(unittest.TestCase, CommonVars):
    packet_name = 'test_export_data'

    def test_export_data(self):
        parser = DBCParams.get_arg_parser()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--unlock'
        ]), self.conf_file).run()

        dbc = MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01
        ]), self.conf_file)
        res = dbc.run()

        self.assertTrue(res.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res.result_code[self.test_dbc_01] == ResultCode.SUCCESS)

        self.assertTrue(len(dbc.export_results.csv_files) > 0)
        for csv_file in dbc.export_results.csv_files:
            self.assertFalse(os.path.exists(csv_file))

        with pyzipper.AESZipFile(dbc.export_results.zip_file) as zip:
            password = os.path.basename(dbc.export_results.zip_file).split('_')[1]
            zip.pwd = password.encode('utf-8')
            for f in zip.infolist():
                current_file = os.path.basename(f.filename)

                output_file_name = next(
                    fv for fv in dbc.export_results.csv_files if os.path.basename(fv) == current_file
                )

                output_file = open(output_file_name, 'w+b')
                output_file.write(zip.read(f))
                output_file.close()

        for csv_file in dbc.export_results.csv_files:
            self.assertTrue(os.path.exists(csv_file))

        for csv_file in dbc.export_results.csv_files:
            os.remove(csv_file)

        os.remove(dbc.export_results.zip_file)
        del dbc.export_results.csv_files[:]


class TestDBCPyStep(unittest.TestCase, CommonVars):
    packet_name = 'test_py_step'

    def test_py_step(self):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01
        ])

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--wipe'
        ]), self.conf_file).run()

        main = MainRoutine(args, self.conf_file)
        res_2 = main.run()

        self.assertTrue(res_2.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res_2.result_code[self.test_dbc_01] == ResultCode.SUCCESS)

        db_local = postgresql.open(main.sys_conf.dbs_dict[self.test_dbc_01])
        content = get_resultset(db_local, """
            SELECT content
            FROM public.test_tbl_import
            WHERE fname in ('data_a.txt', 'data_b.txt')
            ORDER BY id
        """)
        self.assertTrue(content[0][0] == 'Some raw data A')
        self.assertTrue(content[1][0] == 'Some raw data B')
        db_local.close()


class TestDBCCloneSchema(unittest.TestCase, CommonVars):
    test_packet_name = 'test_dba_clone_schema'
    dba_packet_name = 'dba_clone_schema'

    def test_clone_schema(self):
        parser = DBCParams.get_arg_parser()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.test_packet_name,
            '--db-name=' + self.test_dbc_01,
            '--wipe'
        ]), self.conf_file).run()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.dba_packet_name,
            '--db-name=' + self.test_dbc_01,
            '--wipe'
        ]), self.conf_file).run()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.dba_packet_name,
            '--db-name=' + self.test_dbc_01
        ]), self.conf_file).run()

        args = parser.parse_args([
            '--packet-name=' + self.test_packet_name,
            '--db-name=' + self.test_dbc_01
        ])

        main = MainRoutine(args, self.conf_file)
        res_2 = main.run()

        self.assertTrue(res_2.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res_2.result_code[self.test_dbc_01] == ResultCode.SUCCESS)


class TestDBCOverrideConfParam(unittest.TestCase, CommonVars):
    packet_name = 'test_override_conf_param'
    schema_location = 'dbc_a'

    def test_override_conf_param(self):
        parser = DBCParams.get_arg_parser()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--conf={"schema_location":"%s"}' % self.schema_location,
            '--unlock',
        ]), self.conf_file).run()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--conf={"schema_location":"%s"}' % self.schema_location,
            '--wipe',
        ]), self.conf_file).run()

        def run_meta_test(
                statement_timeout,
                packet_status,
                result_code,
                res_status,
                exception_descr
        ):
            args = parser.parse_args([
                '--packet-name=' + self.packet_name,
                '--db-name=' + self.test_dbc_01,
                '--conf={"statement_timeout":"%s","schema_location":"%s"}' % (statement_timeout, self.schema_location),
                '--skip-step-cancel'
            ])

            main = MainRoutine(args, self.conf_file)
            res = main.run()

            self.assertTrue(res.packet_status[self.test_dbc_01] == packet_status)
            self.assertTrue(res.result_code[self.test_dbc_01] == result_code)

            db_local = postgresql.open(main.sys_conf.dbs_dict[self.test_dbc_01])
            dbc_packets_content = get_resultset(
                db_local,
                """SELECT status, meta_data
                FROM %s.dbc_packets where name = '%s'""" % (self.schema_location, self.packet_name)
            )
            self.assertTrue(dbc_packets_content[0][0] == res_status)
            dbc_steps_content = get_resultset(
                db_local,
                """SELECT s.status, exception_descr
                FROM %s.dbc_steps s
                JOIN %s.dbc_packets p on p.id = s.packet_id
                WHERE p.name = '%s'""" % (self.schema_location, self.schema_location, self.packet_name)
            )
            self.assertTrue(dbc_steps_content[0][0] == res_status)
            self.assertTrue(dbc_steps_content[0][1] == exception_descr)
            db_local.close()

        run_meta_test('1s', PacketStatus.EXCEPTION, ResultCode.FAIL, 'exception', 'skip_step')
        run_meta_test('1h', PacketStatus.DONE, ResultCode.SUCCESS, 'done', None)


class TestDBCPlaceholders(unittest.TestCase, CommonVars):
    packet_name = 'test_placeholders'

    def test_placeholders(self):
        parser = DBCParams.get_arg_parser()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--wipe',
        ]), self.conf_file).run()

        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.test_dbc_01,
            '--placeholders={"USER_NAME":"dbc_test_user","PASSW":"1234"}',
        ])

        main = MainRoutine(args, self.conf_file)
        res = main.run()

        self.assertTrue(res.packet_status[self.test_dbc_01] == PacketStatus.DONE)
        self.assertTrue(res.result_code[self.test_dbc_01] == ResultCode.SUCCESS)

        db_local = postgresql.open(main.sys_conf.dbs_dict[self.test_dbc_01])
        dbc_packets_content = get_resultset(
            db_local,
            """
                SELECT count(1)
                FROM pg_roles
                WHERE rolcanlogin = true AND rolname = 'dbc_test_user'
            """
        )
        self.assertTrue(dbc_packets_content[0][0] == 1)
        db_local.close()


class TestDBCAllSeq(unittest.TestCase, CommonVars):
    packet_name = 'test_get_version'

    @mock.patch('matterhook.incoming.requests.post', side_effect=mocked_requests_post)
    def test_all_seq(self, mocked_requests_post):
        db_name = 'ALL,exclude:%s,%s,%s' % (self.test_dbc_01, self.test_dbc_packets, self.pg_db)
        parser = DBCParams.get_arg_parser()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + db_name,
            '--unlock',
        ]), self.conf_file).run()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + db_name,
            '--wipe',
        ]), self.conf_file).run()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + db_name,
            '--list',
        ]), self.conf_file).run()

        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + db_name,
            '--conf={"log_level":"Debug", "log_sql": "True"}',
        ])

        main = MainRoutine(args, self.conf_file)
        res = main.run()

        self.assertTrue(res.packet_status[self.test_dbc_02] == PacketStatus.DONE)
        self.assertTrue(res.result_code[self.test_dbc_02] == ResultCode.SUCCESS)


class TestDBCPacketWithTestData(unittest.TestCase, CommonVars):
    test_packet_names = []

    def setUp(self):
        del self.test_packet_names[:]

        self.test_packet_names = [
            (f[5:], f) for f in os.listdir(self.packets_dir)
            if f.startswith('test_') and os.path.isdir(os.path.join(self.packets_dir, f)) and
                os.path.isdir(os.path.join(self.packets_dir, f[5:])) and
                f not in ('test_dba_clone_schema')
        ]
        self.test_packet_names.sort()

    def test_packets(self):
        parser = DBCParams.get_arg_parser()

        args = parser.parse_args([
            # set any --packet-name for initialization
            '--packet-name=test_common', '--db-name=' + self.test_dbc_packets, '--list'
        ])
        dbc = MainRoutine(args, self.conf_file)

        def cleanup():
            db_conn = postgresql.open(dbc.sys_conf.dbs_dict['pg_db'])

            db_conn.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                    AND datname = '%s'
            """ % self.test_dbc_packets)

            db_conn.execute("""DROP DATABASE IF EXISTS %s""" % self.test_dbc_packets)
            db_conn.execute("""
                CREATE DATABASE %s
                    WITH
                    OWNER = postgres
                    ENCODING = 'UTF8'
                    LC_COLLATE = 'en_US.UTF-8'
                    LC_CTYPE = 'en_US.UTF-8'
                    TABLESPACE = pg_default
                    template = template0""" % self.test_dbc_packets)
            db_conn.close()

        for packet in self.test_packet_names:
            print("TestDBCPacketWithTestData: testing %s -> %s" % (packet[0], packet[1]))
            cleanup()
            # run test packet
            MainRoutine(parser.parse_args([
                '--packet-name=' + packet[1],
                '--db-name=' + self.test_dbc_packets
            ]), self.conf_file).run()

            # run main packet
            res_main_packet = MainRoutine(parser.parse_args([
                '--packet-name=' + packet[0],
                '--db-name=' + self.test_dbc_packets
            ]), self.conf_file).run()

            for step, step_result in res_main_packet.result_data[self.test_dbc_packets].items():
                step_result_text = to_json(step_result, formatted=True)
                # if *.sql_out_orig not exists then save result of main packet to *.sql_out_orig
                if not os.path.isfile(os.path.join(self.packets_dir, packet[1], step + "_out")):
                    print("Creating: %s" % (step + "_out"))
                    out_file = open(os.path.join(self.packets_dir, packet[1], step + "_out"), "w", encoding="utf8")
                    out_file.write(step_result_text)
                    out_file.close()
                else:
                    orig_out_file = open(
                        os.path.join(self.packets_dir, packet[1], step + "_out"),
                        "r",
                        encoding="utf8"
                    )
                    orig_out = orig_out_file.read()
                    orig_out_file.close()
                    if orig_out != step_result_text:
                        orig_out_list = orig_out.splitlines()
                        step_result_text_list = step_result_text.splitlines()
                        del_items = []
                        for row_num, row in enumerate(orig_out_list):
                            if row.find("<SKIP>") > -1:
                                del_items.append(row_num)
                        orig_out_list = [i for j, i in enumerate(orig_out_list) if j not in del_items]
                        step_result_text_list = [i for j, i in enumerate(step_result_text_list) if j not in del_items]
                        validation_failed = False
                        for line in difflib.unified_diff(orig_out_list, step_result_text_list):
                            print(line.rstrip())
                            validation_failed = True
                        if validation_failed:
                            print("Validating: %s FAIL" % step)
                            self.assertTrue(False)
                        else:
                            print("Validating: %s OK" % step)
                    else:
                        print("Validating: %s OK" % step)


if __name__ == '__main__':
    call_TestDBCPrepareDBs = False
    unittest.main(defaultTest="TestDBCPrepareDBs", exit=False)
    unittest.main()
