import unittest
from psc.psccommon.psc_common import *
import time


class TestDBCCommon(unittest.TestCase):

    def test_common(self):
        limit_memory(1024 * 1000 * 500)

        current_dir = os.path.dirname(os.path.realpath(__file__))
        test_dir = os.path.join(current_dir, 'test_dir')
        try:
            os.rmdir(test_dir)
        except FileNotFoundError:
            print("%s not exists" % test_dir)

        prepare_dirs(current_dir, [test_dir])

        self.assertTrue(os.path.isdir(test_dir))

        some_obj = {"a": 1, "b": 2}
        res = to_json(some_obj, formatted=True)
        cmp_res = \
"""{
    "a": 1,
    "b": 2
}"""
        self.assertTrue(res == cmp_res)
