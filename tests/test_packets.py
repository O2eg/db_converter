import unittest
import os
from db_converter import MainRoutine
from db_converter import PacketStatus
from db_converter import CommandType
from db_converter import ResultCode
import psc.postgresql as postgresql
from actiontracker import ActionTracker


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)


class TestPackets(unittest.TestCase):
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
            if f.startswith('test_') and os.path.isdir(os.path.join(packets_dir, f))
        ]:
            args = dict(
                packet_name=f_name,
                db_name='test_dbc',
                template=None,
                list=None,
                status=None,
                stop=None,
                wipe=True,
                seq=False,
                force=False
            )
            self.wipes.append(Struct(**args))

            args = dict(
                packet_name=f_name,
                db_name='test_dbc',
                template=None,
                list=None,
                status=None,
                stop=None,
                wipe=None,
                seq=False,
                force=False
            )

            self.runs.append([
                Struct(**args),
                True if f_name.find('exception') == -1 else False
            ])

    def test_packets(self):
        for args in self.wipes:
            self.assertTrue(MainRoutine(args, self.conf_file).run())

        for args in self.runs:
            if args[1] is True:
                self.assertTrue(MainRoutine(args[0], self.conf_file).run())
            if args[1] is False:
                self.assertFalse(MainRoutine(args[0], self.conf_file).run())


class TestDBCLock(unittest.TestCase):
    wipe_params = None
    run_params = None
    conf_file = 'db_converter_test.conf'
    packet_name = 'test_sleep'
    db_name = 'test_dbc'

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
            force=False
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
            force=False
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


if __name__ == '__main__':
    unittest.main()
