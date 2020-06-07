from psc.pgstatcommon.pg_stat_common import *
import psc.postgresql as postgresql
import threading
import time
import sqlparse
import hashlib
from enum import Enum
from functools import partial
from actiontracker import ActionTracker
from sqlparse.sql import *
import csv
import pyzipper
import string
import random


class BasicEnum:
    def __str__(self):
        return self.value


class PacketType(BasicEnum, Enum):
    DEFAULT = 'default'
    READ_ONLY = 'read_only'
    NO_COMMIT = 'no_commit'
    MAINTENANCE = 'maintenance'
    EXPORT_DATA = 'export_data'


def parse_query_placeholder(query, gen_i, placeholder):
    # placeholder is GEN_NSP_FLD_ or GEN_OBJ_FLD_
    for fld_id, fld in enumerate(dict(gen_i).items()):
        if fld_id > 0:
            fld_val = fld[1]
            query = query.replace(placeholder + str(fld_id), str(fld_val))
    return query


def print_table(table):
    table_text = ""
    col_width = [max(len(str(x)) for x in col) for col in zip(*table)]
    for row_num, row in enumerate(table):
        str_row = "| " + " | ".join("{:{}}".format(x, col_width[i]) for i, x in enumerate(row)) + " |"
        if row_num <= 1:
            row_txt = "-".join(['-' * len(str_row)])
            table_text += row_txt + "\n"
        table_text += str_row + "\n"
    return table_text


class Context:
    def __init__(self, db_name, thread_name, current_pid, packet_name, packet_hash, meta_data, meta_data_json, step):
        self.db_name = db_name
        self.thread_name = thread_name
        self.current_pid = current_pid
        self.packet_name = packet_name
        self.packet_hash = packet_hash
        self.meta_data = meta_data
        self.meta_data_json = meta_data_json
        self.step = step

    def info(self):
        return """Thread '%s', DB '%s', PID %d, Packet '%s', Step '%s'""" % (
            self.thread_name, self.db_name, self.current_pid, self.packet_name, self.step[0]
        )


def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper

# def threaded(fn, callback_func):
#     def wrapper(*args, **kwargs):
#         def do_callback():
#             callback_func(fn(args, kwargs))
#         thread = Thread(target=do_callback)
#         thread.start()
#         return thread
#     return wrapper


class WorkerResult(BasicEnum, Enum):
    SUCCESS = 'success'
    FAIL = 'fail'
    TERMINATE = 'terminate'


class ExportResults:
    csv_files = []
    zip_file = None


class DBCCore:
    lock = threading.Lock()
    workers_db_pid = {}     # key is "db_name", value is array of pids
    worker_threads = {}     # key is "db_name", value is array of threads (one lock_observer and one worker_db_func)
    workers_status = {}     # key is "db_name", boolean value: True is active, False is finished
    workers_result = {}     # key is "db_name", value is WorkerResult
    db_conns = {}
    lock_observer_blocker_cnt = 0
    lock_observer_wait_cnt = 0
    export_results = ExportResults()

    def get_pids(self, db_name):
        try:
            if db_name in self.workers_db_pid:
                yield next(iter(self.workers_db_pid[db_name]))
            else:
                return next(iter([]))
        except StopIteration:
            return

    def remove_pid(self, db_name, pid):
        workers_db_pids = [v for v in self.get_pids(db_name)]
        workers_db_pids.remove(pid)
        self.workers_db_pid[db_name] = workers_db_pids

    def append_pid(self, db_name, pid):
        self.workers_db_pid.setdefault(db_name, []).append(pid)

    def append_thread(self, db_name, thread):
        self.worker_threads.setdefault(db_name, []).append(thread)

    def get_threads(self, db_name):
        return self.worker_threads[db_name]

    def get_num_observed_threads_(self, db_name):
        return self.worker_threads[db_name].num_observed_threads

    def get_worker_status(self, db_name):
        if db_name in self.workers_status:
            return self.workers_status[db_name]
        else:
            return None

    def set_worker_status_start(self, db_name):
        self.workers_status[db_name] = True

    def set_worker_status_finish(self, db_name):
        self.workers_status[db_name] = False

    def set_worker_result(self, db_name, result):
        self.workers_result[db_name] = result

    def interrupt_all_conns(self):
        self.lock.acquire()
        for pid, conn in self.db_conns.items():
            self.logger.log("interrupt_all_conns: stop PID %d" % pid, "Debug", do_print=True)
            conn.interrupt()
        self.lock.release()

    @threaded
    def lock_observer(self, thread_name, str_conn, db_name, app_name_postfix):
        def sleep_lo():
            for i in range(50):
                if self.get_worker_status(db_name) is True or self.get_worker_status(db_name) is None:
                    time.sleep(self.sys_conf.lock_observer_sleep_interval/50)
                    if self.is_terminate:
                        break
                if self.get_worker_status(db_name) is False:
                    break

        self.logger.log(
            'Thread \'%s\' runned! Observed pids: %s' % (thread_name, str([v for v in self.get_pids(db_name)])),
            "Info",
            do_print=True
        )
        do_work = True
        db_conn = None
        while do_work:
            do_work = False
            try:
                db_conn = postgresql.open(str_conn)
                app_name = self.sys_conf.application_name + "_" + app_name_postfix
                db_conn.execute("SET application_name = '%s'" % app_name)

                while len([thread for thread in self.get_threads(db_name) if thread.is_alive()]) > 1 \
                        and self.get_worker_status(db_name) is True and not self.is_terminate:
                    for pid in self.get_pids(db_name):
                        # ===========================================================================
                        # case 1: detect backend activity
                        pid_is_locker = get_scalar(db_conn, """
                            SELECT exists(
                                SELECT 1
                                FROM pg_locks waiting
                                JOIN pg_stat_activity waiting_stm ON waiting_stm.pid = waiting.pid
                                JOIN pg_locks other ON waiting.database = other.database
                                    AND waiting.relation = other.relation
                                    OR waiting.transactionid = other.transactionid
                                JOIN pg_stat_activity other_stm ON other_stm.pid = other.pid
                                WHERE NOT waiting.granted
                                    AND waiting.pid <> other.pid
                                    AND age(clock_timestamp(), waiting_stm.xact_start) > interval %s
                                    AND other.pid = %d
                            )""" % (self.sys_conf.cancel_blocker_tx_timeout, pid)
                        )
                        if pid_is_locker:
                            db_conn.execute("SELECT pg_cancel_backend(%d)" % pid)
                            self.lock.acquire()
                            if pid in self.get_pids(db_name): self.remove_pid(db_name, pid)
                            self.lock.release()
                            self.lock_observer_blocker_cnt += 1
                            self.logger.log('%s: stopped pid %d as blocker' % (thread_name, pid), "Info")
                        # ===========================================================================
                        # case 2: how long to wait for access to relations
                        any_heavyweight_lock_already = get_scalar(db_conn, """
                            SELECT exists(
                                SELECT 1
                                FROM pg_stat_activity
                                WHERE datname = current_database()
                                    AND pid = %d
                                    AND wait_event is not null
                                    AND wait_event_type = 'Lock'
                                    AND pid <> pg_backend_pid()
                                    AND age(clock_timestamp(), xact_start) > interval %s
                            )
                        """ % (pid, self.sys_conf.cancel_wait_tx_timeout))
                        if any_heavyweight_lock_already:
                            db_conn.execute("SELECT pg_cancel_backend(%d)" % pid)
                            self.lock.acquire()
                            if pid in self.get_pids(db_name): self.remove_pid(db_name, pid)
                            self.lock.release()
                            self.lock_observer_wait_cnt += 1
                            self.logger.log('%s: stopped pid %d with heavyweight lock' % (thread_name, pid), "Info")
                        # ===========================================================================
                    self.logger.log(
                        '%s: iteration done. Sleep on %d seconds...' %
                        (thread_name, self.sys_conf.lock_observer_sleep_interval),
                        "Info",
                        do_print=True
                    )
                    self.logger.log(
                        'Thread \'%s\': Observed pids: %s' % (thread_name, str([v for v in self.get_pids(db_name)])),
                        "Info",
                        do_print=True
                    )
                    sleep_lo()
            except (
                    postgresql.exceptions.QueryCanceledError,
                    postgresql.exceptions.AdminShutdownError,
                    postgresql.exceptions.CrashShutdownError,
                    postgresql.exceptions.ServerNotReadyError,
                    AttributeError  # AttributeError: 'Statement' object has no attribute '_row_constructor'
            ) as e:
                if self.is_terminate:
                    self.logger.log('Thread %s stopped!' % thread_name, "Error", do_print=True)
                    if db_conn is not None:
                        db_conn.close()
                    return
                do_work = True
                self.logger.log(
                    'Exception in \'%s\': %s. Reconnecting after %d sec...' %
                    (thread_name, str(e), self.sys_conf.conn_exception_sleep_interval),
                    "Error"
                )
                time.sleep(self.sys_conf.conn_exception_sleep_interval)
            except (
                postgresql.exceptions.AuthenticationSpecificationError,
                postgresql.exceptions.ClientCannotConnectError,
                TimeoutError
            ) as e:
                self.logger.log(
                    'Exception in %s: \n%s' % (thread_name, exception_helper(self.sys_conf.detailed_traceback)),
                    "Error",
                    do_print=True
                )
            finally:
                if db_conn is not None:
                    db_conn.close()
        self.logger.log('Thread %s finished!' % thread_name, "Info", do_print=True)

    def parse_packet(self, packet_name, thread_name):
        step_files = []
        gen_obj_files = {}
        gen_nsp_files = {}
        # parse packet
        try:
            packet_full_content = []
            meta_data = None
            packet_dir = os.path.join(self.sys_conf.current_dir, 'packets', packet_name)
            for step in os.listdir(packet_dir):
                if step.endswith('.sql') or step.endswith('.json'):
                    current_file = open(os.path.join(packet_dir, step), 'r', encoding="utf8")
                    file_content = current_file.read()
                    packet_full_content.append(file_content)
                    current_file.close()

                    if step.endswith('.sql') and step.find('_gen_') == -1:
                        step_files.append([step, file_content])
                    if step.endswith('.sql') and step.find('_gen_obj') != -1:
                        gen_obj_files[step] = file_content
                    if step.endswith('.sql') and step.find('_gen_nsp') != -1:
                        gen_nsp_files[step] = file_content

                    if step == 'meta_data.json':
                        meta_data = file_content

            if meta_data is None:
                meta_data = """{"description":"file meta_data.json not found", "type": "default"}"""

            meta_data_json = json.loads(meta_data)
            meta_data_json['packet_dir'] = os.path.join(
                self.sys_conf.current_dir,
                "packets",
                self.args.packet_name
            )
            # ========================================================================
            # initialize default values
            if "hook" in meta_data_json:
                if "verbosity" not in meta_data_json["hook"]:
                    meta_data_json["hook"]["verbosity"] = ["all"]
            if "type" not in meta_data_json:
                meta_data_json["type"] = "default"
            if "tags" not in meta_data_json:
                tags = []
                for step in step_files:
                    for smt in sqlparse.parse(step[1]):
                        for keyword in [v for v in smt.tokens if v.is_keyword]:
                            tags.append(keyword.value)
                meta_data_json["tags"] = list(set(tags))
            # ========================================================================

            step_files = sorted(step_files, key=lambda val: val[0])
            packet_full_content = sorted(packet_full_content, key=lambda val: val[0])
            packet_full_content_res = ""
            for v in packet_full_content:
                packet_full_content_res += v
            packet_hash = hashlib.md5(packet_full_content_res.encode()).hexdigest()
        except:
            self.logger.log(
                'Exception in \'%s\' (parse_packet): \n%s' %
                    (thread_name, exception_helper(self.sys_conf.detailed_traceback)),
                "Error",
                do_print=True
            )
            return
        return step_files, gen_obj_files, gen_nsp_files, packet_hash, meta_data, meta_data_json

    def prepare_session(self, db_local, meta_data_json):
        def get_param(name):
            if "postgresql" in meta_data_json:
                if name in meta_data_json["postgresql"]:
                    return meta_data_json["postgresql"][name]
            return getattr(self.sys_conf, name)

        is_super = get_scalar(db_local, 'select usesuper from pg_user where usename = CURRENT_USER')
        try:
            if self.sys_conf.log_level == "Debug":
                db_local.execute("SET log_min_error_statement = 'INFO'")
                db_local.execute("SET log_min_duration_statement = 0")
                db_local.execute("SET log_lock_waits = on")
                db_local.execute("SET log_statement = 'all'")
                if is_super:
                    db_local.execute("SET deadlock_timeout = '1h'")
            else:
                if is_super:
                    db_local.execute("SET deadlock_timeout = '%s'" % get_param('deadlock_timeout'))
                db_local.execute("SET statement_timeout = '%s'" % get_param('statement_timeout'))
                db_local.execute("SET vacuum_cost_limit = %d" % get_param('vacuum_cost_limit'))
                db_local.execute("SET work_mem = '%s'" % get_param('work_mem'))
                db_local.execute("SET maintenance_work_mem = '%s'" % get_param('maintenance_work_mem'))
                db_local.execute("SET timezone = '%s'" % get_param('timezone'))
        except (
            postgresql.exceptions.InsufficientPrivilegeError
        ) as e:
            self.logger.log(
                'Exception in prepare_session: %s. Skip configuring session variables...' % str(e),
                "Error"
            )

    @threaded
    def ro_worker_db_func(self, thread_name, db_conn_str, db_name, packet_name):
        self.logger.log("Started '%s' thread for '%s' database" % (thread_name, db_name), "Info")
        self.set_worker_status_start(db_name)

        step_files = []         # vector of pairs [step, sql]
        gen_obj_files = {}      # dict with generators of objects
        gen_nsp_files = {}      # dict with generators of schemas
        steps_hashes = {}       # temporary storage for executed steps
        packet_hash = None
        meta_data = None
        meta_data_json = None

        step_files,\
            gen_obj_files,\
            gen_nsp_files,\
            packet_hash,\
            meta_data,\
            meta_data_json = self.parse_packet(packet_name, thread_name)

        do_work = True
        db_local = None
        current_pid = None
        exception_descr = None
        th_result = False

        while do_work:
            do_work = False
            try:
                # check conn to DB
                if db_local is not None:    # connection db_local has been already established
                    self.logger.log("%s: try 'SELECT 1'" % thread_name, "Info")
                    try:
                        db_local.execute("SELECT 1")    # live?
                        if current_pid not in self.get_pids(db_name):
                            self.append_pid(db_name, current_pid)
                    except:
                        self.logger.log("%s: Connection to DB is broken" % thread_name, "Error")
                        db_local.close()
                        db_local = None     # needs reconnect

                # ======================================================
                # connecting to DB, session variables initialization
                if db_local is None:
                    if current_pid is not None and current_pid in self.get_pids(db_name):
                        self.remove_pid(db_name, current_pid)

                    self.logger.log("Thread '%s': connecting to '%s' database..." % (thread_name, db_name), "Info")
                    db_local = postgresql.open(db_conn_str)
                    db_local.execute(
                        "SET application_name = '%s'" %
                        (self.sys_conf.application_name + "_" + os.path.splitext(packet_name)[0])
                    )

                    self.prepare_session(db_local, meta_data_json)

                    current_pid = get_scalar(db_local, "SELECT pg_backend_pid()")
                    self.db_conns[current_pid] = db_local
                    self.append_pid(db_name, current_pid)
                    self.logger.log(
                        "Thread '%s': connected to '%s' database with pid %d" %
                        (thread_name, db_name, current_pid),
                        "Info"
                    )
                # ======================================================
                def ro_steps_processing():
                    # return format: (result, exception_descr, do_work)
                    for num, step in enumerate(step_files):
                        ctx = Context(db_name, thread_name, current_pid, packet_name,
                                      packet_hash, meta_data, meta_data_json, step)
                        progress = str(round(float(num) * 100 / len(step_files), 2)) + "%"
                        self.logger.log(
                            '%s: progress %s' % (ctx.info(), progress),
                            "Info",
                            do_print=True
                        )
                        result, exception_descr = self.execute_ro_step(
                            ctx,
                            db_local,
                            gen_nsp_data,
                            gen_obj_data,
                            steps_hashes
                        )
                        if result == 'exception' and exception_descr == 'connection':
                            # transaction cancelled or connection stopped
                            time.sleep(self.sys_conf.conn_exception_sleep_interval)
                            return None, None, True
                        if result == 'exception' and exception_descr == 'skip_step':
                            self.logger.log(
                                'Thread \'%s\' (steps_processing): step %s skipped!' %
                                (thread_name, step[0]),
                                "Error",
                                do_print=True
                            )
                        if result == 'exception' and exception_descr is not None and \
                                exception_descr not in('connection', 'skip_step'):
                            return result, exception_descr, False
                        if result == 'terminate':
                            return 'terminate', None, False
                    return True, None, False

                # ===========================================================================
                # read only steps processing
                gen_obj_data = {}
                gen_nsp_data = {}

                for step, query in gen_obj_files.items():
                    gen_obj_data[step.replace("_gen_obj", "_step")] = get_resultset(db_local, query)
                for step, query in gen_nsp_files.items():
                    gen_nsp_data[step.replace("_gen_nsp", "_step")] = get_resultset(db_local, query)

                th_result, exception_descr, do_work = ro_steps_processing()
                # ===========================================================================
            except (
                    postgresql.exceptions.QueryCanceledError,
                    postgresql.exceptions.AdminShutdownError,
                    postgresql.exceptions.CrashShutdownError,
                    postgresql.exceptions.ServerNotReadyError,
                    postgresql.exceptions.DeadlockError,
                    AttributeError  # AttributeError: 'Statement' object has no attribute '_row_constructor'
            ) as e:
                if self.is_terminate:
                    self.logger.log(
                        "Terminated '%s' thread for '%s' database" % (thread_name, db_name),
                        "Error",
                        do_print=True
                    )
                    do_work = False
                else:
                    do_work = True
                    self.logger.log(
                        'Exception in \'%s\': %s. Reconnecting after %d sec...' %
                        (thread_name, str(e), self.sys_conf.conn_exception_sleep_interval),
                        "Error"
                    )
            except:
                do_work = False
                self.set_worker_result(db_name, WorkerResult.FAIL)
                exception_descr = exception_helper(self.sys_conf.detailed_traceback)
                msg = 'Exception in \'%s\' %s on processing packet \'%s\': \n%s' % \
                      (thread_name, str(current_pid), packet_name, exception_descr)
                self.logger.log(msg, "Error", do_print=True)

        if db_local is not None:
            db_local.close()

        self.lock.acquire()
        if current_pid is not None and current_pid in self.get_pids(db_name):
            self.remove_pid(db_name, current_pid)
        self.lock.release()

        self.logger.log("Finished '%s' thread for '%s' database" % (thread_name, db_name), "Info")
        if exception_descr is None and th_result is True:
            self.set_worker_result(db_name, WorkerResult.SUCCESS)
            self.logger.log(
                '<-------- Packet \'%s\' finished for \'%s\' database!' % \
                    (self.args.packet_name, db_name),
                "Info",
                do_print=True
            )
        else:
            if th_result == 'terminate':
                self.set_worker_result(db_name, WorkerResult.TERMINATE)
            else:
                self.set_worker_result(db_name, WorkerResult.FAIL)
            self.logger.log(
                '<-------- Packet \'%s\' failed for \'%s\' database!' % \
                    (self.args.packet_name, db_name),
                "Error",
                do_print=True
            )
        self.set_worker_status_finish(db_name)

    @threaded
    def worker_db_func(self, thread_name, db_conn_str, db_name, packet_name):
        self.logger.log("Started '%s' thread for '%s' database" % (thread_name, db_name), "Info")
        self.set_worker_status_start(db_name)

        step_files = []         # vector of pairs [step, sql]
        gen_obj_files = {}      # dict with generators of objects
        gen_nsp_files = {}      # dict with generators of schemas
        steps_hashes = {}       # temporary storage for executed steps
        packet_hash = None
        meta_data = None

        step_files,\
            gen_obj_files,\
            gen_nsp_files,\
            packet_hash,\
            meta_data,\
            meta_data_json = self.parse_packet(packet_name, thread_name)

        do_work = True
        work_breaked = False
        db_local = None
        current_pid = None
        exception_descr = None
        th_result = False

        while do_work:
            do_work = False
            try:
                # check conn to DB
                if db_local is not None:    # connection db_local has been already established
                    self.logger.log("%s: try 'SELECT 1'" % thread_name, "Info")
                    try:
                        db_local.execute("SELECT 1")    # live?
                        if current_pid not in self.get_pids(db_name):
                            self.append_pid(db_name, current_pid)
                    except:
                        self.logger.log("%s: Connection to DB is broken" % thread_name, "Error")
                        db_local.close()
                        db_local = None     # needs reconnect

                # ======================================================
                # connecting to DB, session variables initialization
                if db_local is None:
                    if current_pid is not None and current_pid in self.get_pids(db_name):
                        self.remove_pid(db_name, current_pid)

                    self.logger.log("Thread '%s': connecting to '%s' database..." % (thread_name, db_name), "Info")
                    db_local = postgresql.open(db_conn_str)
                    db_local.execute(
                        "SET application_name = '%s'" %
                        (self.sys_conf.application_name + "_" + os.path.splitext(packet_name)[0])
                    )

                    self.prepare_session(db_local, meta_data_json)

                    current_pid = get_scalar(db_local, "SELECT pg_backend_pid()")
                    self.db_conns[current_pid] = db_local
                    self.append_pid(db_name, current_pid)
                    self.logger.log(
                        "Thread '%s': connected to '%s' database with pid %d" %
                        (thread_name, db_name, current_pid),
                        "Info"
                    )
                # ======================================================
                if not self.args.force:
                    packet_status = ActionTracker.get_packet_status(db_local, packet_name)
                    if "hash" in packet_status:
                        if packet_status["hash"] != packet_hash:
                            do_work = False
                            self.logger.log(
                                'Thread \'%s\': hash of \'%s\' packet has been changed! Use "--force" option. Stopping...' %
                                (thread_name, packet_name),
                                "Error",
                                do_print=True
                            )
                            work_breaked = True
                            break

                def steps_processing(run_once=False):
                    # return format: (result, exception_descr, do_work)
                    total_steps = len(step_files)
                    for num, step in enumerate(step_files):
                        if run_once and step[0] == 'run_once.sql' or run_once is False:
                            ctx = Context(db_name, thread_name, current_pid, packet_name,
                                          packet_hash, meta_data, meta_data_json, step)
                            progress = str(round(float(num) * 100 / len(step_files), 2)) + "%"
                            self.logger.log(
                                '%s: progress %s' % (ctx.info(), progress),
                                "Info",
                                do_print=True
                            )
                            result, exception_descr = self.execute_step(
                                ctx,
                                db_local,
                                gen_nsp_data,
                                gen_obj_data,
                                steps_hashes
                            )
                            if result == 'exception' and exception_descr == 'connection':
                                # transaction cancelled or connection stopped
                                time.sleep(self.sys_conf.conn_exception_sleep_interval)
                                return None, None, True
                            if result == 'exception' and exception_descr == 'skip_step':
                                self.logger.log(
                                    'Thread \'%s\' (steps_processing): step %s skipped!' %
                                    (thread_name, step[0]),
                                    "Error",
                                    do_print=True
                                )
                            if result == 'done' and exception_descr is None:
                                # step successfully complete
                                ActionTracker.set_step_status(db_local, packet_name, step[0], result)
                            if result == 'exception' and exception_descr is not None and \
                                    exception_descr not in('connection', 'skip_step'):
                                # syntax exception or pre/post check raised exception
                                ActionTracker.set_step_exception_status(db_local, packet_name, step[0], exception_descr)
                                return result, exception_descr, False
                            if result == 'terminate':
                                return 'terminate', None, False
                    return True, None, False

                # ===========================================================================
                # steps processing
                gen_obj_data = {}
                gen_nsp_data = {}

                _, exception_descr, do_work = steps_processing(run_once=True)

                for step, query in gen_obj_files.items():
                    gen_obj_data[step.replace("_gen_obj", "_step")] = get_resultset(db_local, query)
                for step, query in gen_nsp_files.items():
                    gen_nsp_data[step.replace("_gen_nsp", "_step")] = get_resultset(db_local, query)

                th_result, exception_descr, do_work = steps_processing()
                # ===========================================================================
            except (
                    postgresql.exceptions.QueryCanceledError,
                    postgresql.exceptions.AdminShutdownError,
                    postgresql.exceptions.CrashShutdownError,
                    postgresql.exceptions.ServerNotReadyError,
                    postgresql.exceptions.DeadlockError,
                    AttributeError  # AttributeError: 'Statement' object has no attribute '_row_constructor'
            ) as e:
                if self.is_terminate:
                    self.logger.log(
                        "Terminated '%s' thread for '%s' database" % (thread_name, db_name),
                        "Error",
                        do_print=True
                    )
                    do_work = False
                else:
                    do_work = True
                    self.logger.log(
                        'Exception in \'%s\': %s. Reconnecting after %d sec...' %
                        (thread_name, str(e), self.sys_conf.conn_exception_sleep_interval),
                        "Error",
                        do_print=True
                    )
            except (
                postgresql.exceptions.AuthenticationSpecificationError,
                postgresql.exceptions.ClientCannotConnectError
            ) as e:
                self.logger.log(
                    'Exception in %s: \n%s' % (thread_name, exception_helper(self.sys_conf.detailed_traceback)),
                    "Error",
                    do_print=True
                )
            except:
                do_work = False
                self.set_worker_result(db_name, WorkerResult.FAIL)
                exception_descr = exception_helper(self.sys_conf.detailed_traceback)
                msg = 'Exception in \'%s\' %d on processing packet \'%s\': \n%s' % \
                      (thread_name, current_pid, packet_name, exception_descr)
                self.logger.log(msg, "Error", do_print=True)

        if not work_breaked and self.errors_count == 0:
            ActionTracker.set_packet_status(db_local, packet_name, 'done' if exception_descr is None else 'exception')

        if not work_breaked and self.errors_count > 0:
            ActionTracker.set_packet_status(db_local, packet_name, 'exception')

        if db_local is not None:
            db_local.close()

        self.lock.acquire()
        if current_pid is not None and current_pid in self.get_pids(db_name):
            self.remove_pid(db_name, current_pid)
        self.lock.release()

        self.logger.log("Finished '%s' thread for '%s' database" % (thread_name, db_name), "Info")
        if exception_descr is None and th_result is True:
            self.set_worker_result(db_name, WorkerResult.SUCCESS)
            self.logger.log(
                '<-------- Packet \'%s\' finished for \'%s\' database!' % \
                    (self.args.packet_name, db_name),
                "Info",
                do_print=True
            )
        else:
            if th_result == 'terminate':
                self.set_worker_result(db_name, WorkerResult.TERMINATE)
            else:
                self.set_worker_result(db_name, WorkerResult.FAIL)
            self.logger.log(
                '<-------- Packet \'%s\' failed for \'%s\' database!' % \
                    (self.args.packet_name, db_name),
                "Error",
                do_print=True
            )
        self.set_worker_status_finish(db_name)

    def resultset_hook(self, ctx, results):
        try:
            if "hook" in ctx.meta_data_json and len(results) > 0:
                if ctx.meta_data_json["hook"]["type"] == "matterhook" and \
                        hasattr(self, 'matterhooks') and self.matterhooks is not None:
                    msg = "#### :gear: %s: %s `->` %s\n" % (ctx.db_name, ctx.packet_name, ctx.step[0])
                    if "message" in ctx.meta_data_json["hook"]:
                        msg += ctx.meta_data_json["hook"]["message"]

                    if "show_parameters" in ctx.meta_data_json["hook"] and \
                            ctx.meta_data_json["hook"]["show_parameters"] in ("true", "True", "1"):
                        msg += "\n #### Parameters: \n"
                        msg += "```\n"
                        for arg in vars(self.args):
                            msg += '%s = %s\n' % (arg, getattr(self.args, arg))
                        msg += "```"

                    any_item = False
                    for result in results:
                        if isinstance(result, tuple):                                   # verbosity = stm_result
                            if "stm_result" in ctx.meta_data_json["hook"]["verbosity"] or \
                                    "all" in ctx.meta_data_json["hook"]["verbosity"]:
                                msg += "\n```\n" + str(result) + "\n```"
                                any_item = True
                        if isinstance(result, list) and len(result) > 0 and \
                                result[0] in self.sys_conf.plsql_raises:                 # verbosity = raise
                            if "raise" in ctx.meta_data_json["hook"]["verbosity"] or \
                                    "all" in ctx.meta_data_json["hook"]["verbosity"]:
                                msg += "\n```\n%s: %s\n```" % (result[0], result[1])
                                any_item = True
                        if isinstance(result, list) and len(result) > 0 and \
                                result[0] not in self.sys_conf.plsql_raises:             # verbosity = resultset
                            if "resultset" in ctx.meta_data_json["hook"]["verbosity"] or \
                                    "all" in ctx.meta_data_json["hook"]["verbosity"]:
                                table = []
                                table.append(list(result[0].column_names))
                                table_content = []

                                for row in result:
                                    table_content.append([str(v) for v in row])

                                table.extend(table_content)
                                msg += "\n```\n" + print_table(table) + "\n```"
                                any_item = True

                    if any_item:
                        self.matterhooks[ctx.meta_data_json["hook"]["channel"]].send(
                            msg,
                            channel=ctx.meta_data_json["hook"]["channel"],
                            username=ctx.meta_data_json["hook"]["username"]
                            if "username" in ctx.meta_data_json["hook"] else "db_converter"
                        )
        except:
            exception_descr = exception_helper(self.sys_conf.detailed_traceback)
            self.logger.log(
                '%s: Exception in "resultset_hook" %s' % (ctx.info, exception_descr),
                "Error",
                do_print=True
            )

    @staticmethod
    def generate_password(length=12):
        printable = f'{string.ascii_letters}{string.digits}'
        printable = list(printable)
        random.shuffle(printable)
        random_password = random.choices(printable, k=length)
        random_password = ''.join(random_password)
        return random_password

    def is_maint_query(self, query):
        is_maint = True
        for op in self.sys_conf.maint_ops:
            cmds = op.split('%')
            match = 0
            if len(cmds) > 1:
                for cmd in cmds:
                    if re.search(r"\b" + re.escape(cmd) + r"\b", query):
                        match += 1
                if len(cmds) == match:
                    return True
            elif re.search(r"\b" + re.escape(op) + r"\b", query):
                return True
        return False

    def export_data(self, ctx, conn, stms):
        token_types = []
        stms_is_export = False

        for stm in stms:
            if len(sqlparse.parse(stm)) > 0:
                for token in sqlparse.parse(stm)[0].tokens:
                    if token.ttype in (sqlparse.tokens.DML, sqlparse.tokens.DDL):
                        token_types.append((token.ttype, token.normalized))

        # check statement for export data
        token_types = set(token_types)
        if len(token_types) == 1:
            op_type = next(iter(token_types))
            if op_type[0] == sqlparse.tokens.DML and op_type[1] == 'SELECT':
                self.logger.log("%s: export data started..." % (ctx.info()), "Info", do_print=True)
                with conn.xact(isolation='REPEATABLE READ') as xact:
                    conn.execute("SET TRANSACTION READ ONLY")
                    for stm in stms:
                        cursor = conn.prepare(stm).declare()
                        resultset = cursor.read(10000)
                        column_names = []
                        if len(list(resultset[0].column_names)) != len(list(resultset[0])):
                            column_names = ['?column?'] * len(list(resultset[0]))
                        else:
                            column_names = list(resultset[0].column_names)

                        try:
                            output_file_name = 'export_%s_%s_%s.csv' % (
                                hashlib.md5(stm.encode()).hexdigest()[0:6],
                                time.strftime("%Y%m%d-%H%M%S"),
                                ctx.db_name
                            )
                            output_file_name = os.path.join(ctx.meta_data_json['packet_dir'], output_file_name)
                            self.export_results.csv_files.append(output_file_name)
                            with open(output_file_name, 'w', newline='') as csvfile:
                                writer = csv.writer(csvfile, delimiter='	', quoting=csv.QUOTE_ALL)
                                writer.writerow(column_names)
                                for row in resultset:
                                    writer.writerow([str(v) for v in row])

                                while len(resultset) > 0:
                                    resultset = cursor.read(10000)
                                    for row in resultset:
                                        writer.writerow([str(v) for v in row])
                            stms_is_export = True
                        except:
                            exception_descr = exception_helper(self.sys_conf.detailed_traceback)
                            self.logger.log(
                                'Exception in "export_data" %s: \n%s' % (ctx.info(), exception_descr),
                                "Error",
                                do_print=True
                            )
                        finally:
                            cursor.close()
                self.logger.log("%s: export data finished!" % (ctx.info()), "Info", do_print=True)

        if stms_is_export:
            if 'export_options' in ctx.meta_data_json and 'use_zip' in ctx.meta_data_json['export_options']:
                secret_password = None
                try:
                    if 'password' in ctx.meta_data_json['export_options'] and \
                            ctx.meta_data_json['export_options']['password'] == 'random':
                        secret_password = self.generate_password()
                        self.export_results.zip_file = os.path.join(
                            ctx.meta_data_json['packet_dir'],
                            'export_%s_%s_%s.zip' % (
                                secret_password, time.strftime("%Y%m%d-%H%M%S"), ctx.db_name
                            )
                        )
                    elif 'password' in ctx.meta_data_json['export_options']:
                        secret_password = ctx.meta_data_json['export_options']['password']
                        self.export_results.zip_file = os.path.join(
                            ctx.meta_data_json['packet_dir'],
                            'export_%s_%s.zip' % (time.strftime("%Y%m%d-%H%M%S"), ctx.db_name)
                        )
                    else:
                        self.export_results.zip_file = os.path.join(
                            ctx.meta_data_json['packet_dir'],
                            'export_%s_%s.zip' % (time.strftime("%Y%m%d-%H%M%S"), ctx.db_name)
                        )

                    if secret_password is not None:
                        with pyzipper.AESZipFile(self.export_results.zip_file, 'w',
                                                 compression=pyzipper.ZIP_LZMA,
                                                 encryption=pyzipper.WZ_AES) as zf:
                            zf.setpassword(secret_password.encode('utf-8'))
                            for csv_file in self.export_results.csv_files:
                                zf.write(csv_file)
                    else:
                        with pyzipper.ZipFile(self.export_results.zip_file, 'w', compression=pyzipper.ZIP_LZMA) as zf:
                            for csv_file in self.export_results.csv_files:
                                zf.write(csv_file)

                    for csv_file in self.export_results.csv_files:
                        os.remove(csv_file)
                except:
                    exception_descr = exception_helper(self.sys_conf.detailed_traceback)
                    self.logger.log(
                        'Exception in "export_data" %s: \n%s' % (ctx.info(), exception_descr),
                        "Error",
                        do_print=True
                    )

        # if statement(s) is not SELECT or mixed: INSERT, ALTER, etc... return False
        return stms_is_export

    def execute_q(self, ctx, conn, query, isolation_level="READ COMMITTED", read_only=False):
        results = []

        if "client_min_messages" in ctx.meta_data_json:
            # conn.execute("set client_min_messages = 'NOTICE'")
            conn.settings['client_min_messages'] = ctx.meta_data_json["client_min_messages"]

        def filter_notices(msg, msgs_list):
            if msg.details['severity'] in self.sys_conf.plsql_raises:
                msgs_list.append([msg.details['severity'], msg.message])
                self.logger.log('%s: %s' % (msg.details['severity'], msg.message), "Info", do_print=True)
                return True

        conn.msghook = partial(filter_notices, msgs_list=results)

        try:
            if self.is_maint_query(query.lower()):
                self.logger.log("%s Executing as maintenance query:\n%s" % (ctx.info(), query), "Info", do_print=True)
                conn.execute(query)
            else:
                stms = sqlparse.split(query)
                stms_is_export = False
                if ctx.meta_data_json['type'] == PacketType.EXPORT_DATA.value:
                    stms_is_export = self.export_data(ctx, conn, stms)
                if not stms_is_export or not(ctx.meta_data_json['type'] == PacketType.EXPORT_DATA.value):
                    with conn.xact(isolation=isolation_level) as xact:
                        # ReadOnlyTransactionError: cannot execute ... in a read-only transaction
                        if read_only:
                            conn.execute("SET TRANSACTION READ ONLY")

                        for stm in stms:
                            prepared = conn.prepare(stm)
                            res = prepared()
                            results.append(res)
                            # ===============================================================================
                            # output to stdout
                            if isinstance(res, tuple):
                                self.logger.log('%s' % str(res), "Info", do_print=True)
                            if isinstance(res, list) and len(res) > 0 and res[0] not in self.sys_conf.plsql_raises:
                                table = []
                                if len(list(res[0].column_names)) != len(list(res[0])):
                                    table.append(['?column?'] * len(list(res[0])))
                                else:
                                    table.append(list(res[0].column_names))
                                table_content = []

                                for row in res:
                                    table_content.append([str(v) for v in row])

                                table.extend(table_content)
                                table_text = print_table(table)
                                self.logger.log('\n%s' % str(table_text), "Info", do_print=True)
                            # ===============================================================================

                            if ctx.meta_data_json["type"] == PacketType.NO_COMMIT.value:
                                self.logger.log("%s: Performing rollback..." % (ctx.info()), "Info")
                                xact.rollback()
        except postgresql.exceptions.OperationError:
            self.logger.log("%s: Transaction aborted" % (ctx.info()), "Info", do_print=True)

        # output via hook
        self.resultset_hook(ctx, results)

    def execute_step(
            self,
            ctx,
            db_local,
            gen_nsp_data,
            gen_obj_data,
            steps_hashes
        ):
        exception_descr = None
        step_hash = None
        execute_step_do_work = True
        enable_at = True if ctx.meta_data_json["type"] == PacketType.DEFAULT.value else False

        while execute_step_do_work:
            execute_step_do_work = False
            try:
                # case 1: both generators is exists
                if ctx.step[1].find("GEN_NSP_FLD_") > -1 and ctx.step[1].find("GEN_OBJ_FLD_") > -1:
                    if ctx.step[0] not in gen_obj_data:
                        self.logger.log(
                            "%s: not found generator for this step, but GEN_OBJ_FLD_ is exists" % (ctx.info()),
                            "Error"
                        )
                        raise Exception(msg)
                    if ctx.step[0] not in gen_nsp_data:
                        self.logger.log(
                            "%s: not found generator for this step, but GEN_NSP_FLD_ is exists" % (ctx.info()),
                            "Error"
                        )
                        raise Exception(msg)
                    for gen_nsp_i in gen_nsp_data[ctx.step[0]]:  # namespace generators have a major priority
                        for gen_obj_i in gen_obj_data[ctx.step[0]]:  # object generators have a minor priority
                            gen_query = parse_query_placeholder(
                                parse_query_placeholder(ctx.step[1], gen_nsp_i, 'GEN_NSP_FLD_'),
                                gen_obj_i, 'GEN_OBJ_FLD_'
                            )
                            step_hash = hashlib.md5(gen_query.encode()).hexdigest()
                            if step_hash in steps_hashes:
                                continue
                            if enable_at and ActionTracker.is_action_exists(
                                    db_local, ctx.packet_name, ctx.step[0], step_hash
                            ):
                                steps_hashes[step_hash] = ctx.step[0]
                                self.logger.log(
                                    "%s: action already executed with hash %s" % (ctx.info(), step_hash),
                                    "Info"
                                )
                            else:
                                # ========================================================================
                                if self.sys_conf.log_sql == 1:
                                    self.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")
                                if self.sys_conf.execute_sql:
                                    if enable_at:
                                        ActionTracker.begin_action(
                                            db_local, ctx.packet_name, ctx.packet_hash, ctx.step[0], ctx.meta_data
                                        )
                                    self.execute_q(ctx, db_local, gen_query)
                                    if enable_at:
                                        ActionTracker.apply_action(
                                            db_local, ctx.packet_name, ctx.step[0], step_hash
                                        )
                                    steps_hashes[step_hash] = ctx.step[0]
                                    self.logger.log("%s: action finished" % (ctx.info()), "Info")

                                if gen_nsp_i[0] is not None and len(str(gen_nsp_i[0])) > 0:  # run maintenance command
                                    if self.sys_conf.log_sql == 1:
                                        self.logger.log("%s:\n%s" % (ctx.info(), gen_nsp_i[0]), "Info")
                                    if self.sys_conf.execute_sql:
                                        self.execute_q(ctx, db_local, gen_nsp_i[0])

                                if gen_obj_i[0] is not None and len(str(gen_obj_i[0])) > 0:  # run maintenance command
                                    if self.sys_conf.log_sql == 1:
                                        self.logger.log("%s:\n%s" % (ctx.info(), gen_obj_i[0]), "Info")
                                    if self.sys_conf.execute_sql:
                                        self.execute_q(ctx, db_local, gen_obj_i[0])
                                # ========================================================================
                # case 2: only OBJ generator is exists
                if ctx.step[1].find("GEN_NSP_FLD_") == -1 and ctx.step[1].find("GEN_OBJ_FLD_") > -1:
                    if ctx.step[0] not in gen_obj_data:
                        msg = "%s: not found generator for this step, but GEN_OBJ_FLD_ is exists" % (ctx.info())
                        self.logger.log(msg, "Error")
                        raise Exception(msg)
                    for gen_obj_i in gen_obj_data[ctx.step[0]]:
                        gen_query = parse_query_placeholder(ctx.step[1], gen_obj_i, 'GEN_OBJ_FLD_')
                        step_hash = hashlib.md5(gen_query.encode()).hexdigest()
                        if step_hash in steps_hashes:
                            continue
                        if enable_at and ActionTracker.is_action_exists(
                                db_local,
                                ctx.packet_name,
                                ctx.step[0],
                                step_hash
                        ):
                            steps_hashes[step_hash] = ctx.step[0]
                            self.logger.log(
                                "%s: already executed with hash %s" % (ctx.info(), step_hash),
                                "Info"
                            )
                        else:
                            # ========================================================================
                            if self.sys_conf.log_sql == 1:
                                self.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")
                            if self.sys_conf.execute_sql:
                                if enable_at:
                                    ActionTracker.begin_action(
                                        db_local, ctx.packet_name, ctx.packet_hash, ctx.step[0], ctx.meta_data
                                    )
                                self.execute_q(ctx, db_local, gen_query)
                                if enable_at:
                                    ActionTracker.apply_action(db_local, ctx.packet_name, ctx.step[0], step_hash)
                                steps_hashes[step_hash] = ctx.step[0]
                                self.logger.log("%s: action finished" % (ctx.info()), "Info")

                            if gen_obj_i[0] is not None and len(str(gen_obj_i[0])) > 0:  # run maintenance command
                                if self.sys_conf.log_sql == 1:
                                    self.logger.log("%s:\n%s" % (ctx.info(), gen_obj_i[0]), "Info")
                                if self.sys_conf.execute_sql:
                                    self.execute_q(ctx, db_local, gen_obj_i[0])

                            # ========================================================================
                # case 3: only NSP generator is exists
                if ctx.step[1].find("GEN_NSP_FLD_") > -1 and ctx.step[1].find("GEN_OBJ_FLD_") == -1:
                    if ctx.step[0] not in gen_nsp_data:
                        msg = "%s: not found generator for this step, but GEN_NSP_FLD_ is exists" % (ctx.info())
                        self.logger.log(msg, "Error")
                        raise Exception(msg)
                    for gen_nsp_i in gen_nsp_data[ctx.step[0]]:
                        gen_query = parse_query_placeholder(ctx.step[1], gen_nsp_i, 'GEN_NSP_FLD_')
                        step_hash = hashlib.md5(gen_query.encode()).hexdigest()
                        if step_hash in steps_hashes:
                            continue
                        if enable_at and ActionTracker.is_action_exists(db_local, ctx.packet_name, ctx.step[0], step_hash):
                            steps_hashes[step_hash] = ctx.step[0]
                            self.logger.log("%s: action already executed with hash %s" % (ctx.info(), step_hash), "Info")
                        else:
                            # ========================================================================
                            if self.sys_conf.log_sql == 1:
                                self.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")
                            if self.sys_conf.execute_sql:
                                if enable_at: ActionTracker.begin_action(
                                    db_local, ctx.packet_name, ctx.packet_hash, ctx.step[0], ctx.meta_data
                                )
                                self.execute_q(ctx, db_local, gen_query)
                                if enable_at: ActionTracker.apply_action(db_local, ctx.packet_name, ctx.step[0], step_hash)
                                steps_hashes[step_hash] = ctx.step[0]
                                self.logger.log("%s: action finished" % (ctx.info()), "Info")

                            if gen_nsp_i[0] is not None and len(str(gen_nsp_i[0])) > 0:  # run maintenance command
                                if self.sys_conf.log_sql == 1:
                                    self.logger.log("%s:\n%s" % (ctx.info(), gen_nsp_i[0]), "Info")
                                if self.sys_conf.execute_sql:
                                    self.execute_q(ctx, db_local, gen_nsp_i[0])
                            # ========================================================================
                # case 4: no generators
                if ctx.step[1].find("GEN_NSP_FLD_") == -1 and ctx.step[1].find("GEN_OBJ_FLD_") == -1:
                    step_hash = hashlib.md5(ctx.step[1].encode()).hexdigest()
                    if step_hash not in steps_hashes:
                        if enable_at and ActionTracker.is_action_exists(db_local, ctx.packet_name, ctx.step[0], step_hash):
                            steps_hashes[step_hash] = ctx.step[0]
                            self.logger.log(
                                "%s: action already executed with hash %s" % (ctx.info(), step_hash),
                                "Info"
                            )
                        else:
                            # ========================================================================
                            if self.sys_conf.log_sql == 1:
                                self.logger.log("%s:\n%s" % (ctx.info(), ctx.step[1]), "Info")
                            if self.sys_conf.execute_sql:
                                if enable_at: ActionTracker.begin_action(
                                    db_local, ctx.packet_name, ctx.packet_hash, ctx.step[0], ctx.meta_data
                                )
                                self.execute_q(ctx, db_local, ctx.step[1])
                                if enable_at:
                                    ActionTracker.apply_action(db_local, ctx.packet_name, ctx.step[0], step_hash)
                                steps_hashes[step_hash] = ctx.step[0]
                                self.logger.log("%s: action finished" % (ctx.info()), "Info")
                            # ========================================================================
            except (
                postgresql.exceptions.PLPGSQLRaiseError
            ):
                self.raise_error_logic(ctx)
            except (
                    postgresql.exceptions.QueryCanceledError,
                    postgresql.exceptions.AdminShutdownError,
                    postgresql.exceptions.CrashShutdownError,
                    postgresql.exceptions.ServerNotReadyError,
                    postgresql.exceptions.DeadlockError,
                    AttributeError     # AttributeError: 'Statement' object has no attribute '_row_constructor'
            ) as e:
                '''
                57014	query_canceled
                57P01	admin_shutdown
                57P02	crash_shutdown
                57P03	cannot_connect_now
                57P04	database_dropped
                40000	transaction_rollback
                40P01	deadlock_detected
                '''
                if self.is_terminate:
                    return 'terminate', None
                self.logger.log(
                    'Exception in %s (execute_step): %s. Reconnecting after %d sec...' %
                    (ctx.info(), str(e), self.sys_conf.conn_exception_sleep_interval),
                    "Error",
                    do_print=True
                )
                time.sleep(self.sys_conf.conn_exception_sleep_interval)
                if self.args.skip_step_cancel:
                    return 'exception', 'skip_step'
                elif self.args.skip_action_cancel:
                    steps_hashes[step_hash] = ctx.step[0]
                    self.logger.log(
                        '%s (execute_step): action %s in step %s skipped!' %
                        (ctx.info(), step_hash, ctx.step[0]),
                        "Error",
                        do_print=True
                    )
                    self.errors_count += 1
                    execute_step_do_work = True
                elif e.code == '40P01':
                    return 'exception', 'deadlock_detected'
                else:
                    return 'exception', 'connection'
            except:
                exception_descr = exception_helper(self.sys_conf.detailed_traceback)
                self.logger.log(
                    'Exception in "execute_step" %s: \n%s' % (ctx.info(), exception_descr),
                    "Error",
                    do_print=True
                )
                return 'exception', exception_descr

        return 'done', None

    def raise_error_logic(self, ctx):
        try:
            if "hook" in ctx.meta_data_json:
                if ctx.meta_data_json["hook"]["type"] == "matterhook" and self.matterhooks is not None:
                    msg = "#### :comet: %s: %s `->` %s\n" % (ctx.db_name, ctx.packet_name, ctx.step[0])
                    msg += ctx.meta_data_json["hook"]["message"]
                    exc_type, exc_value, _ = sys.exc_info()
                    msg += "\n``` bash\n" + str(exc_value) + "\n```"

                    if "show_parameters" in ctx.meta_data_json["hook"] and \
                            ctx.meta_data_json["hook"]["show_parameters"] in ("true", "True", "1"):
                        msg += "\n #### Parameters: \n"
                        msg += "```\n"
                        for arg in vars(self.args):
                            msg += '%s = %s\n' % (arg, getattr(self.args, arg))
                        msg += "```"

                    self.matterhooks[ctx.meta_data_json["hook"]["channel"]].send(
                        msg,
                        channel=ctx.meta_data_json["hook"]["channel"],
                        username=ctx.meta_data_json["hook"]["username"]
                        if "username" in ctx.meta_data_json["hook"] else "db_converter"
                    )
        except:
            exception_descr = exception_helper(self.sys_conf.detailed_traceback)
            self.logger.log(
                'Exception in "raise_error_logic" %s: \n%s' % (ctx.info(), exception_descr),
                "Error",
                do_print=True
            )

    def execute_ro_step(
            self,
            ctx,
            db_local,
            gen_nsp_data,
            gen_obj_data,
            steps_hashes
        ):
        exception_descr = None
        step_hash = None
        execute_step_do_work = True

        def execute_ro(conn, query):
            return self.execute_q(ctx, conn, query, isolation_level='REPEATABLE READ', read_only=True)

        while execute_step_do_work:
            execute_step_do_work = False
            try:
                # case 1: both generators is exists
                if ctx.step[1].find("GEN_NSP_FLD_") > -1 and ctx.step[1].find("GEN_OBJ_FLD_") > -1:
                    for gen_nsp_i in gen_nsp_data[ctx.step[0]]:  # namespace generators have a major priority
                        for gen_obj_i in gen_obj_data[ctx.step[0]]:  # object generators have a minor priority
                            gen_query = parse_query_placeholder(
                                parse_query_placeholder(ctx.step[1], gen_nsp_i, 'GEN_NSP_FLD_'),
                                gen_obj_i, 'GEN_OBJ_FLD_'
                            )
                            step_hash = hashlib.md5(gen_query.encode()).hexdigest()
                            if step_hash in steps_hashes:
                                continue

                            # ========================================================================
                            if self.sys_conf.log_sql == 1:
                                self.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")
                            if self.sys_conf.execute_sql:
                                execute_ro(db_local, gen_query)
                                steps_hashes[step_hash] = ctx.step[0]
                                self.logger.log("%s: action finished" % (ctx.info()), "Info")

                            if gen_nsp_i[0] is not None and len(str(gen_nsp_i[0])) > 0:  # run maintenance command
                                if self.sys_conf.log_sql == 1:
                                    self.logger.log("%s:\n%s" % (ctx.info(), gen_nsp_i[0]), "Info")

                                if self.sys_conf.execute_sql:
                                    execute_ro(db_local, gen_nsp_i[0])

                            if gen_obj_i[0] is not None and len(str(gen_obj_i[0])) > 0:  # run maintenance command
                                if self.sys_conf.log_sql == 1:
                                    self.logger.log("%s:\n%s" % (ctx.info(), gen_obj_i[0]), "Info")

                                if self.sys_conf.execute_sql:
                                    execute_ro(db_local, gen_obj_i[0])
                                # ========================================================================
                # case 2: only OBJ generator is exists
                if ctx.step[1].find("GEN_NSP_FLD_") == -1 and ctx.step[1].find("GEN_OBJ_FLD_") > -1:
                    for gen_obj_i in gen_obj_data[ctx.step[0]]:
                        gen_query = parse_query_placeholder(ctx.step[1], gen_obj_i, 'GEN_OBJ_FLD_')
                        step_hash = hashlib.md5(gen_query.encode()).hexdigest()
                        if step_hash in steps_hashes:
                            continue

                        # ========================================================================
                        if self.sys_conf.log_sql == 1:
                            self.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")

                        if self.sys_conf.execute_sql:
                            execute_ro(db_local, gen_query)
                            steps_hashes[step_hash] = ctx.step[0]
                            self.logger.log("%s: action finished" % (ctx.info()), "Info")

                        if gen_obj_i[0] is not None and len(str(gen_obj_i[0])) > 0:  # run maintenance command
                            if self.sys_conf.log_sql == 1:
                                self.logger.log("%s:\n%s" % (ctx.info(), gen_obj_i[0]), "Info")
                            if self.sys_conf.execute_sql:
                                execute_ro(db_local, gen_obj_i[0])
                        # ========================================================================
                # case 3: only NSP generator is exists
                if ctx.step[1].find("GEN_NSP_FLD_") > -1 and ctx.step[1].find("GEN_OBJ_FLD_") == -1:
                    for gen_nsp_i in gen_nsp_data[ctx.step[0]]:
                        gen_query = parse_query_placeholder(ctx.step[1], gen_nsp_i, 'GEN_NSP_FLD_')
                        step_hash = hashlib.md5(gen_query.encode()).hexdigest()
                        if step_hash in steps_hashes:
                            continue

                        # ========================================================================
                        if self.sys_conf.log_sql == 1:
                            self.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")

                        if self.sys_conf.execute_sql:
                            execute_ro(db_local, gen_query)
                            steps_hashes[step_hash] = ctx.step[0]
                            self.logger.log("%s: action finished" % (ctx.info()), "Info")

                        if gen_nsp_i[0] is not None and len(str(gen_nsp_i[0])) > 0:  # run maintenance command
                            if self.sys_conf.log_sql == 1:
                                self.logger.log("%s:\n%s" % (ctx.info(), gen_nsp_i[0]), "Info")

                            if self.sys_conf.execute_sql:
                                execute_ro(db_local, gen_nsp_i[0])
                        # ========================================================================
                # case 4: no generators
                if ctx.step[1].find("GEN_NSP_FLD_") == -1 and ctx.step[1].find("GEN_OBJ_FLD_") == -1:
                    step_hash = hashlib.md5(ctx.step[1].encode()).hexdigest()
                    if step_hash not in steps_hashes:
                            # ========================================================================
                            if self.sys_conf.log_sql == 1:
                                self.logger.log("%s:\n%s" % (ctx.info(), ctx.step[1]), "Info")

                            if self.sys_conf.execute_sql:
                                execute_ro(db_local, ctx.step[1])
                                steps_hashes[step_hash] = ctx.step[0]
                                self.logger.log("%s: action finished" % (ctx.info()), "Info")
                            # ========================================================================
            except (
                    postgresql.exceptions.PLPGSQLRaiseError,
                    postgresql.exceptions.ReadOnlyTransactionError
            ):
                self.raise_error_logic(ctx)
            except (
                    postgresql.exceptions.QueryCanceledError,
                    postgresql.exceptions.AdminShutdownError,
                    postgresql.exceptions.CrashShutdownError,
                    postgresql.exceptions.ServerNotReadyError,
                    postgresql.exceptions.DeadlockError,
                    AttributeError     # AttributeError: 'Statement' object has no attribute '_row_constructor'
            ) as e:
                '''
                57014	query_canceled
                57P01	admin_shutdown
                57P02	crash_shutdown
                57P03	cannot_connect_now
                57P04	database_dropped
                40000	transaction_rollback
                40P01	deadlock_detected
                '''
                if self.is_terminate:
                    return 'terminate', None
                self.logger.log(
                    'Exception in %s (execute_ro_step): %s. Reconnecting after %d sec...' %
                    (ctx.info(), str(e), self.sys_conf.conn_exception_sleep_interval),
                    "Error",
                    do_print=True
                )
                time.sleep(self.sys_conf.conn_exception_sleep_interval)
                if self.args.skip_step_cancel:
                    return 'exception', 'skip_step'
                elif self.args.skip_action_cancel:
                    steps_hashes[step_hash] = ctx.step[0]
                    self.logger.log(
                        '%s (execute_ro_step): action %s in step %s skipped!' %
                        (ctx.info(), step_hash, ctx.step[0]),
                        "Error",
                        do_print=True
                    )
                    self.errors_count += 1
                    execute_step_do_work = True
                elif e.code == '40P01':
                    return 'exception', 'deadlock_detected'
                else:
                    return 'exception', 'connection'
            except:
                exception_descr = exception_helper(self.sys_conf.detailed_traceback)
                self.logger.log(
                    'Exception in %s "execute_ro_step": \n%s' % (ctx.info(), exception_descr),
                    "Error",
                    do_print=True
                )
                return 'exception', exception_descr

        return 'done', None
