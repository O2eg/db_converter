import time
import configparser
import argparse
from psc.pgstatlogger.pg_stat_logger import PSCLogger
from psc.pgstatcommon.pg_stat_common import *
from psc.postgresql import exceptions
import psc.postgresql as postgresql
import logging
from matterhook import Webhook
import json
from actiontracker import ActionTracker
from dbccore import *
import shutil
from enum import Enum

VERSION = 2.6


class SysConf:
    plsql_raises = ['DEBUG', 'LOG', 'INFO', 'NOTICE', 'WARNING', 'EXCEPTION']
    maint_ops = ['concurrently', 'vacuum', 'analyze']

    def __init__(self, conf):
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.config = configparser.RawConfigParser()
        self.config.optionxform = lambda option: option
        self.config.read(
            os.path.join(
                self.current_dir,
                'conf',
                os.path.splitext(os.path.basename(__file__))[0] + ".conf" if conf is None else conf
            )
        )

        def get_key(section, parameter, default, boolean=False):
            try:
                return read_conf_param_value(self.config[section][parameter], boolean)
            except KeyError:
                return default

        self.dbs_dict = {}
        for db in self.config['databases']:
            self.dbs_dict[db] = read_conf_param_value(self.config['databases'][db])

        # main parameters
        self.application_name = get_key('main', 'application_name', 'db_converter')
        self.execute_sql = get_key('main', 'execute_sql', 'True')
        self.lock_observer_sleep_interval = int(
            get_key('main', 'lock_observer_sleep_interval', '5')
        )
        self.conn_exception_sleep_interval = int(
            get_key('main', 'conn_exception_sleep_interval', '5')
        )
        self.cancel_blocker_tx_timeout = get_key('main', 'cancel_blocker_tx_timeout', '5 seconds')
        self.cancel_wait_tx_timeout = get_key('main', 'cancel_wait_tx_timeout', '5 seconds')
        self.detailed_traceback = get_key('main', 'detailed_traceback', 'True', boolean=True)
        self.db_name_all_confirmation = get_key('main', 'db_name_all_confirmation', 'True', boolean=True)

        # log parameters
        self.log_level = get_key('log', 'log_level', 'Info')
        self.log_sql = int(get_key('log', 'log_sql', '1'))
        self.file_maxmbytes = int(get_key('log', 'file_maxmbytes', '50'))
        self.file_backupcount = int(get_key('log', 'file_backupcount', '5'))

        # session parameters
        self.deadlock_timeout = get_key('postgresql', 'deadlock_timeout', '100ms')
        self.statement_timeout = get_key('postgresql', 'statement_timeout', '1h')
        self.vacuum_cost_limit = int(get_key('postgresql', 'vacuum_cost_limit', '3000'))
        self.work_mem = get_key('postgresql', 'work_mem', '200MB')
        self.maintenance_work_mem = get_key('postgresql', 'maintenance_work_mem', '1GB')
        self.timezone = get_key('postgresql', 'timezone', 'UTC')

        self.matterhook_conf = {}
        try:
            self.matterhook_conf["url"] = read_conf_param_value(self.config['matterhook']['url'])
            chats_keys = read_conf_param_value(self.config['matterhook']['chat_keys']).split(',')
            chats_keys_dict = {}
            for chat_key in chats_keys:
                item = chat_key.split('/')
                chats_keys_dict[item[0]] = item[1]

            self.matterhook_conf["chat_keys"] = chats_keys_dict
        except KeyError:
            self.matterhook_conf = None


class DBCParams:
    sys_conf = None
    logger = None
    args = None
    is_terminate = False
    matterhooks = None
    errors_count = 0

    @staticmethod
    def get_arg_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--packet-name",
            help="Select specific packet name in 'packets' directory",
            type=str
        )
        parser.add_argument(
            "--db-name",
            help="Select DB name from [databases] section of 'conf/db_converter.conf'",
            type=str,
            default="ALL"
        )
        parser.add_argument(
            "--status",
            help="Show packet status",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--force",
            help="Ignore the difference between packet hashes",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--skip-step-cancel",
            help="Skip whole step on first error like Deadlock, QueryCanceledError",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--skip-action-cancel",
            help="Skip action errors like Deadlock, QueryCanceledError",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--seq",
            help="Sequential execution in order listed in 'db_converter.conf'",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--wipe",
            help="Delete information about '--packet-name' from action tracker (dbc_* tables)",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--stop",
            help="Execute pg_terminate_backend for all db_converter connections with specific packet name",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--unlock",
            help="Unlock the specified packet",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--version",
            help="Show the version number and exit",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--list",
            help="List all databases according --db-name mask",
            action='store_true',
            default=False
        )
        parser.add_argument(
            "--template",
            help="Copy *.sql files from 'packets/templates/template' to 'packets/packet-name'",
            type=str
        )
        return parser

    def __init__(self, args, conf):
        try:
            if args is None:
                try:
                    self.args = self.get_arg_parser().parse_args()
                except SystemExit:
                    sys.exit(0)
                except:
                    print(exception_helper())
                    sys.exit(0)

                if not len(sys.argv) > 1:
                    print("No arguments. Type -h for help.")
                    sys.exit(0)
            else:
                self.args = args

            if hasattr(self.args, 'version') and self.args.version:
                print("Version %s" % VERSION)
                sys.exit(0)

            self.sys_conf = SysConf(conf)

            packet_dir = os.path.join(self.sys_conf.current_dir, 'packets', self.args.packet_name)
            if not os.path.isdir(packet_dir) or len(self.args.packet_name) == 0:
                print("Invalid --packet-name=%s" % self.args.packet_name)
                sys.exit(0)

            if self.args.packet_name.isdigit():
                print("Wrong format of --packet-name=%s: only digits are not allowed!" % self.args.packet_name)
                sys.exit(0)

            if len(self.args.packet_name) < 3:
                print("Wrong format of --packet-name=%s: minimal length is 3 symbols!" % self.args.packet_name)
                sys.exit(0)

            prepare_dirs(self.sys_conf.current_dir, ["log"])

            # ========================================================================
            # check --template arg
            # How to run test: --packet-name=test_template --db-name=* --template=test
            if self.args.template is not None:
                template_dir = os.path.join(
                    self.sys_conf.current_dir,
                    "packets",
                    "templates",
                    self.args.template
                )

                if os.path.isdir(template_dir):
                    for f_name in [
                        f for f in os.listdir(template_dir)
                            if f.endswith('.sql') and os.path.isfile(os.path.join(template_dir, f))
                    ]:
                        print("Copying %s to %s" % (f_name, packet_dir))
                        shutil.copy(os.path.join(template_dir, f_name), packet_dir)
                else:
                    print("Directory %s is not exists!" % template_dir)
                    sys.exit(0)
            # ========================================================================

            replace_symbols = ['*', '?', ':']
            log_name_part = self.args.db_name
            for s in replace_symbols:
                log_name_part = log_name_part.replace(s, "_")
            self.logger = PSCLogger.instance(
                "dbc_" + log_name_part + "_" + self.args.packet_name,
                log_level=logging._nameToLevel[self.sys_conf.log_level.upper()],
                max_bytes=1024*1000*self.sys_conf.file_maxmbytes,
                backup_count=self.sys_conf.file_backupcount,
                delay=1
            )

            if self.sys_conf.matterhook_conf is not None:
                self.matterhooks = {}
                for chat, key in self.sys_conf.matterhook_conf["chat_keys"].items():
                    self.matterhooks[chat] = Webhook(self.sys_conf.matterhook_conf["url"], key)

            else:
                self.matterhook = None
        except SystemExit as e:
            print("Exiting...")
            sys.exit(0)
        except:
            print("Can't initialize application. Exiting...")
            print(exception_helper())
            sys.exit(0)


class CommandType(BasicEnum, Enum):
    LIST = 'list'
    WIPE = 'wipe'
    RUN = 'run'
    STOP = 'stop'
    STATUS = 'status'
    UNLOCK = 'unlock'


class PacketStatus(BasicEnum, Enum):
    DONE = 'done'
    STARTED = 'started'
    EXCEPTION = 'exception'
    NEW = 'new'         # if record not exists in "dbc_packets" table
    UNKNOWN = 'unknown'


class ResultCode:
    SUCCESS = 'success'
    FAIL = 'fail'
    LOCKED = 'locked'
    NOTHING_TODO = 'nothing_todo'
    TERMINATE = 'terminate'


class DBCResult:
    command_type = None
    packet_type = None
    result_code = None
    packet_status = None


class MainRoutine(DBCParams, DBCCore):
    external_interrupt = False
    command_type = None         # CommandType
    packet_type = None          # PacketType
    result_code = {}            # {db_name: ResultCode}
    packet_status = {}          # {db_name: PacketStatus}
    db_packet_status = {}       #

    # db_packet_status = {
    #     status: 'done | started | exception',
    #     exception_descr: 'text',
    #     exception_dt: 'datetime',
    #     hash: 'text'
    # }

    dbs = []        # lists of databases for processing

    def terminate_conns(self, db_conn, db_name, app_name, packet, terminate=True):
        found_conns = False
        cmd = 'pg_terminate_backend' if terminate else 'pg_cancel_backend'
        print("============================================")
        print("Database: %s \n" % db_name)
        for rec in get_resultset(db_conn, """
            SELECT datname,
                   pid, 
                   client_addr,
                   %s(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
              AND datname = current_database()
              AND application_name = '%s_%s'
            """ % (cmd, app_name, packet)
        ):
            print('%s     %s      %s      %s' % (rec[0], rec[1], rec[2], rec[3]))
            found_conns = True
        if not found_conns:
            print('No connections')
        print("============================================")
        return found_conns

    def init_command_type(self):
        # initialize command_type
        if self.args.list:
            self.command_type = CommandType.LIST
        elif self.args.wipe:
            self.command_type = CommandType.WIPE
        elif self.args.stop:
            self.command_type = CommandType.STOP
        elif self.args.status:
            self.command_type = CommandType.STATUS
        else:
            self.command_type = CommandType.RUN

    def init_packet_type(self):
        # initialize packet_type
        meta_data_file_name = os.path.join(
            self.sys_conf.current_dir,
            "packets",
            self.args.packet_name,
            "meta_data.json"
        )

        if os.path.isfile(meta_data_file_name):
            current_file = open(meta_data_file_name, 'r')
            file_content = current_file.read()
            current_file.close()
            try:
                meta_data_json = json.loads(file_content)
                if "type" in meta_data_json:
                    if meta_data_json["type"] == 'default':
                        self.packet_type = PacketType.DEFAULT
                    if meta_data_json["type"] == 'read_only':
                        self.packet_type = PacketType.READ_ONLY
                    if meta_data_json["type"] == 'no_commit':
                        self.packet_type = PacketType.NO_COMMIT
                    if meta_data_json["type"] == 'maintenance':
                        self.packet_type = PacketType.MAINTENANCE
                else:
                    self.packet_type = PacketType.DEFAULT
            except json.decoder.JSONDecodeError:
                raise Exception('WrongMetadata')
        else:
            self.packet_type = PacketType.DEFAULT

    def init_dbs_list(self):
        # processing of "db_name" parameter
        if self.args.db_name == 'ALL':  # all databases
            for db_name, str_conn in self.sys_conf.dbs_dict.items():
                if db_name not in self.dbs:
                    self.dbs.append(db_name)
        elif self.args.db_name.find('ALL,exclude:') == 0:
            all_dbs = [db_name for db_name, _ in self.sys_conf.dbs_dict.items()]
            exclude_dbs = []
            not_dbs_param = self.args.db_name[len('ALL,exclude:'):].split(",")
            for not_db in not_dbs_param:
                if not_db.find("*") == -1:
                    exclude_dbs.append(not_db)
                else:
                    for db in all_dbs:
                        if match(not_db, db) and db not in exclude_dbs:
                            exclude_dbs.append(db)
            for db in all_dbs:
                if db not in exclude_dbs and db not in self.dbs:
                    self.dbs.append(db)
        else:
            dbs_prepare = self.args.db_name.split(",")
            for db in dbs_prepare:
                for db_name, _ in self.sys_conf.dbs_dict.items():
                    if match(db, db_name) and db_name not in self.dbs:
                        self.dbs.append(db_name)

    def __init__(self, args=None, conf=None):
        try:
            super().__init__(args, conf)
            self.init_command_type()
            self.init_packet_type()
            self.init_dbs_list()
        except:
            print("Can't run application. Exiting...")
            print(exception_helper())
            sys.exit(0)

    # helper for iterate all threads
    def iterate_threads(self):
        common_list_of_threads = []
        for db, threads_per_db in self.worker_threads.items():
            common_list_of_threads.extend(threads_per_db)
        for thread_i in common_list_of_threads:
            yield thread_i

    # method for synchronous/asynchronous behaviour
    def wait_threads(self):
        alive_count = 1
        live_iteration = 0
        while alive_count > 0:
            with SignalHandler() as handler:
                alive_count = len([thread for thread in self.iterate_threads() if thread.is_alive()])
                if alive_count == 0: break
                time.sleep(0.1)
                if live_iteration % (20 * 3) == 0:
                    self.logger.log('Live %s threads: %s' % (
                        alive_count,
                        str([thread for thread in self.iterate_threads() if thread.is_alive()])
                    ), "Debug", do_print=True)
                live_iteration += 1
                if handler.interrupted or self.external_interrupt:
                    self.logger.log('Received termination signal! Call interrupt_all_conns...', "Debug", do_print=True)
                    self.is_terminate = True
                    self.interrupt_all_conns()

    def fill_status(self, db_name, db_conn):
        self.db_packet_status = ActionTracker.get_packet_status(db_conn, self.args.packet_name)
        if "status" in self.db_packet_status:
            if "exception_descr" in self.db_packet_status and self.db_packet_status["exception_descr"] is not None:
                self.packet_status[db_name] = PacketStatus.EXCEPTION
            elif "status" in self.db_packet_status and self.db_packet_status["status"] is not None:
                if self.db_packet_status["status"] == 'done':
                    self.packet_status[db_name] = PacketStatus.DONE
                if self.db_packet_status["status"] == 'started':
                    self.packet_status[db_name] = PacketStatus.STARTED
        else:
            self.packet_status[db_name] = PacketStatus.NEW

    def cleanup(self):
        self.result_code.clear()
        self.packet_status.clear()
        self.db_packet_status.clear()
        self.workers_result.clear()
        self.workers_db_pid.clear()
        self.worker_threads.clear()
        self.workers_status.clear()
        self.db_conns.clear()
        del self.dbs[:]
        self.command_type = None
        self.packet_type = None

    # processing specific DB
    def run_on_db(self, db_name, str_conn):
        db_conn = postgresql.open(str_conn)
        # ================================================================================================
        # Call init_tbls for specific args
        if self.packet_type == PacketType.DEFAULT or \
                self.command_type == CommandType.WIPE or \
                self.command_type == CommandType.STATUS:
            ActionTracker.init_tbls(db_conn)
        # ================================================================================================
        if self.packet_type == PacketType.DEFAULT or self.args.status:
            self.fill_status(db_name, db_conn)
        if self.packet_type in (PacketType.READ_ONLY, PacketType.MAINTENANCE, PacketType.NO_COMMIT):
            self.packet_status[db_name] = PacketStatus.NEW
        # ================================================================================================
        if self.args.stop:
            term_conn_res = self.terminate_conns(
                db_conn, db_name, self.sys_conf.application_name, self.args.packet_name
            )
            self.result_code[db_name] = ResultCode.SUCCESS if term_conn_res else ResultCode.NOTHING_TODO
        # ================================================================================================
        if self.args.wipe:
            wipe_res = ActionTracker.wipe_packet(db_conn, self.args.packet_name)
            if wipe_res:
                self.result_code[db_name] = ResultCode.SUCCESS
                print("=====> Database '%s', packet '%s' successfully wiped!" % (db_name, self.args.packet_name))
            else:
                self.result_code[db_name] = ResultCode.NOTHING_TODO
                print("=====> Database '%s', packet '%s' data not found!" % (db_name, self.args.packet_name))
            self.packet_status[db_name] = PacketStatus.NEW
        # ================================================================================================
        if self.args.status:
            print(
                "=====> Database '%s', packet '%s' status: %s" %
                (
                    db_name,
                    self.args.packet_name,
                    "new" if "status" not in self.packet_status else self.packet_status["status"]
                )
            )
            self.result_code[db_name] = ResultCode.SUCCESS

            if "exception_descr" in  self.db_packet_status and  self.db_packet_status["exception_descr"] is not None:
                print("       Action date time: %s" % str( self.db_packet_status["exception_dt"]))
                print("=".join(['=' * 100]))
                print(self.db_packet_status["exception_descr"])
                print("=".join(['=' * 100]))
        # ================================================================================================
        if self.command_type == CommandType.RUN:
            if self.packet_status[db_name] != PacketStatus.DONE:
                # ===========================================
                if ActionTracker.is_packet_locked(db_conn, self.args.packet_name):
                    self.logger.log(
                        '=====> Packet %s is locked in DB %s' % (self.args.packet_name, db_name),
                        "Error",
                        do_print=True
                    )
                    self.result_code[db_name] = ResultCode.LOCKED
                    self.packet_status[db_name] = PacketStatus.STARTED
                else:
                    ActionTracker.set_packet_lock(db_conn, self.args.packet_name)
                    self.logger.log(
                        '=====> Hold lock for packet %s in DB %s' % (self.args.packet_name, db_name),
                        "Info",
                        do_print=True
                    )
                    self.append_thread(
                        db_name,
                        self.lock_observer("lock_observer_%s" % str(db_name), str_conn, db_name, self.args.packet_name)
                    )

                    if self.packet_type == PacketType.READ_ONLY:
                        self.append_thread(
                            db_name,
                            self.ro_worker_db_func(
                                "ro_manager_db_%s" % str(db_name), str_conn, db_name, self.args.packet_name
                            )
                        )
                    else:
                        self.append_thread(
                            db_name,
                            self.worker_db_func(
                                "manager_db_%s" % str(db_name), str_conn, db_name, self.args.packet_name
                            )
                        )

                    self.logger.log(
                        '--------> Packet \'%s\' started for \'%s\' database!' % \
                        (self.args.packet_name, db_name),
                        "Info",
                        do_print=True
                    )
                # ===========================================
            if self.packet_status[db_name] == PacketStatus.DONE:
                self.logger.log(
                    '<-------- Packet \'%s\' already deployed to \'%s\' database!' % \
                    (self.args.packet_name, db_name),
                    "Info",
                    do_print=True
                )
                self.packet_status[db_name] = PacketStatus.DONE
                self.result_code[db_name] = ResultCode.NOTHING_TODO

        if self.args.unlock:
            if ActionTracker.is_packet_locked(db_conn, self.args.packet_name):
                ActionTracker.set_packet_unlock(db_conn, self.args.packet_name)
                self.result_code[db_name] = ResultCode.SUCCESS
            else:
                self.result_code[db_name] = ResultCode.NOTHING_TODO

        db_conn.close()

    def run(self) -> DBCResult:
        self.logger.log('=====> DBC %s started' % VERSION, "Info", do_print=True)

        # ========================================================================
        # confirmation
        break_deployment = False
        if not self.args.list and not self.args.status and self.sys_conf.db_name_all_confirmation:
            if len(self.dbs) > 1 and not self.args.force:
                print("Deployment will be performed on these databases:\n")
                for db_name in self.dbs:
                    print("     " + db_name)
                cmd_question = input('\nDo you want to continue? Type YES to continue...\n')
                if cmd_question != "YES":
                    print('Stopping...')
                    break_deployment = True
                    self.result_code[db_name] = ResultCode.NOTHING_TODO
        # ========================================================================

        if self.args.list:
            print("List of targets:")
            for db_name in self.dbs:
                print("     " + db_name)
                break_deployment = True
                self.result_code[db_name] = ResultCode.NOTHING_TODO

        if not break_deployment:
            if len(self.dbs) == 0:
                self.logger.log('No target databases!', "Error", do_print=True)
                for db in self.args.db_name.split(','):
                    self.packet_status[db] = PacketStatus.UNKNOWN
                    self.result_code[db] = ResultCode.NOTHING_TODO
            for db_name in self.dbs:
                try:
                    self.run_on_db(db_name, self.sys_conf.dbs_dict[db_name])
                    if self.args.seq:
                        self.wait_threads()     # wait on pair (lock_observer, worker_db_func)
                except (
                        postgresql.exceptions.AuthenticationSpecificationError,
                        postgresql.exceptions.ClientCannotConnectError,
                        TimeoutError
                ) as e:
                    self.logger.log(
                        'Cannot connect to %s: \n%s' % (db_name, exception_helper(self.sys_conf.detailed_traceback)),
                        "Error",
                        do_print=True
                    )

        if not break_deployment:
            self.wait_threads()     # wait all threads

        for db, result in self.workers_result.items():
            if db in self.sys_conf.dbs_dict:
                db_conn = postgresql.open(self.sys_conf.dbs_dict[db])
                ActionTracker.set_packet_unlock(db_conn, self.args.packet_name)
                db_conn.close()
                if result == WorkerResult.SUCCESS:
                    self.result_code[db] = ResultCode.SUCCESS
                    self.packet_status[db_name] = PacketStatus.DONE
                if result == WorkerResult.FAIL:
                    self.result_code[db] = ResultCode.FAIL
                    self.packet_status[db_name] = PacketStatus.EXCEPTION
                if result == WorkerResult.TERMINATE:
                    self.result_code[db] = ResultCode.TERMINATE
                    self.packet_status[db_name] = PacketStatus.STARTED

        self.logger.log('<===== DBC %s finished' % VERSION, "Info", do_print=True)
        PSCLogger.instance().stop()

        result = DBCResult()
        result.command_type = self.command_type
        result.packet_type = self.packet_type
        result.result_code = self.result_code.copy()
        result.packet_status = self.packet_status.copy()

        self.cleanup()
        return result


if __name__ == "__main__":
    MainRoutine().run()
