import unittest
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from db_converter import *
import psc.postgresql as postgresql
from actiontracker import ActionTracker


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class TestDBCPackets(unittest.TestCase):
    wipes = []
    runs = []
    conf_file = 'db_converter_test.conf'

    def setUp(self):
        packets_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
            "packets"
        )
        for f_name in [
            f for f in os.listdir(packets_dir)
            if f.startswith('test_') and os.path.isdir(os.path.join(packets_dir, f)) and f not in [
                'test_sleep_sigint',
                'test_skip_step_cancel',
                'test_skip_action_cancel',
                'test_prepare_dbs',
                'test_blocker_tx',
                'test_wait_tx'
            ]
        ]:
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

    def test_packets(self):
        for args in self.wipes:
            res = MainRoutine(args, self.conf_file).run()
            self.assertTrue(
                res.result_code[args.db_name] == ResultCode.NOTHING_TODO or
                res.result_code[args.db_name] == ResultCode.SUCCESS
            )
            self.assertTrue(res.packet_status[args.db_name] == PacketStatus.NEW)

        for args in self.runs:
            res = MainRoutine(args[0], self.conf_file).run()
            if args[1] == ResultCode.SUCCESS:
                self.assertTrue(res.result_code[args[0].db_name] == ResultCode.SUCCESS)
                self.assertTrue(res.packet_status[args[0].db_name] == PacketStatus.DONE)
            if args[1] == ResultCode.FAIL:
                self.assertTrue(res.result_code[args[0].db_name] == ResultCode.FAIL)
                self.assertTrue(res.packet_status[args[0].db_name] == PacketStatus.EXCEPTION)


class TestDBCLock(unittest.TestCase):
    wipe_params = None
    run_params = None
    conf_file = 'db_converter_test.conf'
    packet_name = 'test_sleep'
    db_name = 'test_dbc_01'

    def setUp(self):
        wipe_args = dict(
            packet_name=self.packet_name,
            db_name=self.db_name,
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
            db_name=self.db_name,
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

    def test_lock(self):
        dbc = MainRoutine(self.wipe_params, self.conf_file)
        res_1 = dbc.run()
        self.assertTrue(res_1.packet_status[self.db_name] == PacketStatus.NEW)
        self.assertTrue(res_1.result_code[self.db_name] == ResultCode.NOTHING_TODO)

        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
        ActionTracker.set_packet_lock(db_conn, self.packet_name)

        res_2 = MainRoutine(self.run_params, self.conf_file).run()
        self.assertTrue(res_2.packet_status[self.db_name] == PacketStatus.STARTED)
        self.assertTrue(res_2.result_code[self.db_name] == ResultCode.LOCKED)

        ActionTracker.set_packet_unlock(db_conn, self.packet_name)
        db_conn.close()

        res_3 = MainRoutine(self.run_params, self.conf_file).run()
        self.assertTrue(res_3.packet_status[self.db_name] == PacketStatus.DONE)
        self.assertTrue(res_3.result_code[self.db_name] == ResultCode.SUCCESS)


class TestDBCLockKey(unittest.TestCase):
    conf_file = 'db_converter_test.conf'
    packet_name = 'test_sleep'
    db_name = 'test_dbc_01'

    def test_lock(self):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args(['--packet-name=' + self.packet_name, '--db-name=' + self.db_name])
        dbc = MainRoutine(args, self.conf_file)

        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
        ActionTracker.set_packet_lock(db_conn, self.packet_name)

        res_1 = dbc.run()
        self.assertTrue(res_1.packet_status[self.db_name] == PacketStatus.STARTED)
        self.assertTrue(res_1.result_code[self.db_name] == ResultCode.LOCKED)

        ActionTracker.set_packet_unlock(db_conn, self.packet_name)

        res_2 = MainRoutine(args, self.conf_file).run()
        self.assertTrue(res_2.packet_status[self.db_name] == PacketStatus.DONE)
        self.assertTrue(res_2.result_code[self.db_name] == ResultCode.SUCCESS)

        db_conn.close()


class TestDBCSignal(unittest.TestCase):
    conf_file = 'db_converter_test.conf'
    packet_name = 'test_sleep_sigint'
    db_name = 'test_dbc_01'

    def test_sigint(self):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args(['--packet-name=' + self.packet_name, '--db-name=' + self.db_name])

        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
        ActionTracker.set_packet_unlock(db_conn, self.packet_name)
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
            th_db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
            main.terminate_conns(
                th_db_conn, self.db_name, main.sys_conf.application_name, self.packet_name
            )
            th_db_conn.close()

        if os.name == 'nt':
            main.append_thread(self.db_name + '_ext', emulate_signal())
        else:
            main.append_thread(self.db_name + '_ext', send_signal())

        res = main.run()

        self.assertTrue(res.packet_status[self.db_name] == PacketStatus.STARTED)
        self.assertTrue(res.result_code[self.db_name] == ResultCode.TERMINATE)


class TestDBCUnknownDB(unittest.TestCase):
    conf_file = 'db_converter_test.conf'
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


class TestDBCConnErr(unittest.TestCase):
    conf_file = 'db_converter_test.conf'
    packet_name = 'test_sleep'
    db_name = 'test_dbc_01'

    def test_conn_err(self):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args(['--packet-name=' + self.packet_name, '--db-name=' + self.db_name])

        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
        ActionTracker.init_tbls(db_conn)
        ActionTracker.set_packet_unlock(db_conn, self.packet_name)
        db_conn.close()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def emulate_conn_error():
            time.sleep(2)
            th_db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
            main.terminate_conns(
                th_db_conn, self.db_name, main.sys_conf.application_name, self.packet_name
            )
            th_db_conn.close()

        main.append_thread(self.db_name + '_ext', emulate_conn_error())

        res = main.run()

        self.assertTrue(res.packet_status[self.db_name] == PacketStatus.DONE)
        self.assertTrue(res.result_code[self.db_name] == ResultCode.SUCCESS)


class TestDBCSkipStepCancel(unittest.TestCase):
    conf_file = 'db_converter_test.conf'
    packet_name = 'test_skip_step_cancel'
    db_name = 'test_dbc_01'

    def test_skip_step_cancel(self):
        parser = DBCParams.get_arg_parser()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.db_name,
            '--wipe'
        ]), self.conf_file).run()

        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.db_name,
            '--skip-step-cancel'
        ])

        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
        ActionTracker.set_packet_unlock(db_conn, self.packet_name)
        db_conn.close()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def emulate_conn_error():
            time.sleep(3)
            th_db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
            main.terminate_conns(
                th_db_conn, self.db_name, main.sys_conf.application_name, self.packet_name, terminate=False
            )
            th_db_conn.close()

        main.append_thread(self.db_name + '_ext', emulate_conn_error())

        res_2 = main.run()

        self.assertTrue(res_2.packet_status[self.db_name] == PacketStatus.DONE)
        self.assertTrue(res_2.result_code[self.db_name] == ResultCode.SUCCESS)


class TestDBCSkipActionCancel(unittest.TestCase):
    conf_file = 'db_converter_test.conf'
    packet_name = 'test_skip_action_cancel'
    db_name = 'test_dbc_01'

    def test_skip_action_cancel(self):
        parser = DBCParams.get_arg_parser()

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.db_name,
            '--wipe'
        ]), self.conf_file).run()

        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.db_name,
            '--skip-action-cancel'
        ])

        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
        ActionTracker.set_packet_unlock(db_conn, self.packet_name)
        db_conn.close()

        main = MainRoutine(args, self.conf_file)

        @threaded
        def emulate_conn_error():
            time.sleep(5)
            th_db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
            main.terminate_conns(
                th_db_conn, self.db_name, main.sys_conf.application_name, self.packet_name, terminate=False
            )
            th_db_conn.close()

        main.append_thread(self.db_name + '_ext', emulate_conn_error())

        res_2 = main.run()

        self.assertTrue(res_2.packet_status[self.db_name] == PacketStatus.DONE)
        self.assertTrue(res_2.result_code[self.db_name] == ResultCode.SUCCESS)


class TestDBCPrepareDBs(unittest.TestCase):
    conf_file = 'db_converter_test.conf'
    packet_name = 'test_prepare_dbs'
    db_name = 'pg_db'

    def test_create_db(self):
        global call_TestDBCPrepareDBs
        if call_TestDBCPrepareDBs:
            return

        parser = DBCParams.get_arg_parser()

        args = parser.parse_args([
            '--packet-name=' + self.packet_name, '--db-name=' + self.db_name, '--wipe'
        ])
        dbc = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(dbc.sys_conf.dbs_dict[self.db_name])
        ActionTracker.cleanup(db_conn)

        db_conn.execute("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
                AND datname in ('test_dbc_01', 'test_dbc_02')
        """)

        db_conn.execute("""DROP DATABASE test_dbc_01""")
        db_conn.execute("""DROP DATABASE test_dbc_02""")
        db_conn.close()

        MainRoutine(args, self.conf_file).run()
        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name, '--db-name=' + self.db_name, '--unlock'
        ]), self.conf_file).run()
        res_1 = MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name, '--db-name=' + self.db_name
        ]), self.conf_file).run()

        call_TestDBCPrepareDBs = True


class TestDBCBlockerTxTimeout(unittest.TestCase):
    conf_file = 'db_converter_test.conf'
    packet_name = 'test_blocker_tx'
    db_name = 'test_dbc_01'

    def test_skip_action_cancel(self):
        parser = DBCParams.get_arg_parser()
        args = parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.db_name
        ])

        MainRoutine(parser.parse_args([
            '--packet-name=' + self.packet_name,
            '--db-name=' + self.db_name,
            '--wipe'
        ]), self.conf_file).run()

        main = MainRoutine(args, self.conf_file)
        db_conn = postgresql.open(main.sys_conf.dbs_dict[self.db_name])
        ActionTracker.set_packet_unlock(db_conn, self.packet_name)
        db_conn.close()

        @threaded
        def emulate_workload():
            time.sleep(1)
            th_db_conn = postgresql.open(main.sys_conf.dbs_dict[self.db_name])
            th_db_conn.execute("""vacuum full public.test_blocker_tx_tbl""")
            th_db_conn.close()

        main.append_thread(self.db_name + '_ext', emulate_workload())

        res_2 = main.run()

        self.assertTrue(main.lock_observer_blocker_cnt == 1)
        self.assertTrue(res_2.packet_status[self.db_name] == PacketStatus.DONE)
        self.assertTrue(res_2.result_code[self.db_name] == ResultCode.SUCCESS)


if __name__ == '__main__':
    call_TestDBCPrepareDBs = False
    unittest.main(defaultTest="TestDBCPrepareDBs", exit=False)
    unittest.main()
