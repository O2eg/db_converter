from threading import Thread
import threading
import logging
import logging.handlers
import time
from psc.psccommon.psc_common import *


class PSCLogger(Thread):
    logger = None
    delay = None
    log_queue = []
    log_level = None
    handler = None
    lock_logger = threading.Lock()
    do_stop = False
    __instance = None

    @staticmethod
    def instance(app_name="PSCLogger", log_level=logging.DEBUG, max_bytes=1024*1000*10, backup_count=50, delay=3):
        if PSCLogger.__instance is None:
            PSCLogger(app_name, log_level, max_bytes, backup_count, delay).start()
        return PSCLogger.__instance

    def __init__(self, app_name, log_level=logging.DEBUG, max_bytes=1024*1000*10, backup_count=50, delay=3):
        self.lock_logger.acquire()
        self.logger = logging.getLogger(app_name)
        parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
        self.handler = logging.handlers.RotatingFileHandler(
            os.path.join(parent_dir, 'log', app_name + '.log'),
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)
        self.logger.setLevel(log_level)
        self.delay = delay
        self.log_level = log_level
        PSCLogger.__instance = self
        Thread.__init__(self)
        self.lock_logger.release()

    def flush_data(self):
        try:
            self.lock_logger.acquire()
            for v in self.log_queue:
                if v[0] == 'Error':
                    self.logger.error(str(v[1]))
                if v[0] == 'Warning':
                    self.logger.warning(str(v[1]))
                if v[0] == 'Info':
                    self.logger.info(str(v[1]))
                if v[0] == 'Debug':
                    self.logger.debug(str(v[1]))
            del self.log_queue[:]
            self.lock_logger.release()
        except:
            print('Error in PSCLogger flush_data: \n%s' % exception_helper(self.sys_conf.detailed_traceback))
            self.lock_logger.release()
            self.stop()

    def run(self):
        live_iteration = 0
        while not self.do_stop:
            time.sleep(self.delay/50)
            if live_iteration % 50 == 0 or self.do_stop:
                self.flush_data()
            live_iteration += 1

        if self.log_level == logging.DEBUG:
            print("PSCLogger stopped!")

    def stop(self):
        self.do_stop = True
        self.flush_data()
        self.lock_logger.acquire()
        self.logger.removeHandler(self.handler)
        PSCLogger.__instance = None
        self.lock_logger.release()

    def log(self, msg, code, do_print=False):
        self.lock_logger.acquire()
        if code == 'Debug' and self.log_level == logging.DEBUG:
            self.log_queue.append(['Debug', msg])
            if do_print:
                print(code + ": " + msg)
        if code == 'Info' and self.log_level <= logging.INFO:
            self.log_queue.append(['Info', msg])
            if do_print:
                print(code + ": " + msg)
        if code == 'Warning' and self.log_level <= logging.WARNING:
            self.log_queue.append(['Warning', msg])
            if do_print:
                print(code + ": " + msg)
        if code == 'Error' and self.log_level <= logging.ERROR:
            self.log_queue.append(['Error', msg])
            if do_print:
                print(code + ": " + msg)
        self.lock_logger.release()
