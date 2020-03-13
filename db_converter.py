import time
import threading
import configparser
import argparse
from psc.pgstatlogger.pg_stat_logger import PSCLogger
from psc.pgstatcommon.pg_stat_common import *
import psc.postgresql as postgresql
import logging
import hashlib
from matterhook import Webhook
import json
from enum import Enum
from functools import partial
import sqlparse
from actiontracker import ActionTracker
import shutil

VERSION = 2.4


class BasicEnum:
    def __str__(self):
        return self.value


class PacketType(BasicEnum, Enum):
    DEFAULT = 'default'
    READ_ONLY = 'read_only'
    NO_COMMIT = 'no_commit'
    MAINTENANCE = 'maintenance'


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


class SysConf:
    plsql_raises = ['DEBUG', 'LOG', 'INFO', 'NOTICE', 'WARNING', 'EXCEPTION']
    maint_ops = ['concurrently', 'vacuum', 'analyze']

    def __init__(self):
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.config = configparser.RawConfigParser()
        self.config.optionxform = lambda option: option
        self.config.read(
            os.path.join(
                self.current_dir,
                'conf',
                os.path.splitext(os.path.basename(__file__))[0] + ".conf"
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

        try:
            self.lock_file_dir = read_conf_param_value(self.config['main']['lock_file_dir'])
            if not os.path.isdir(self.lock_file_dir):
                print("Invalid lock_file_dir = %s" % self.lock_file_dir)
                sys.exit(0)
        except KeyError:
            self.lock_file_dir = None

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


def terminate_conns(db_conn, db_name, app_name, packet):
    found_conns = False
    print("============================================")
    print("Database: %s \n" % db_name)
    for rec in get_resultset(db_conn, """
        SELECT datname,
               pid, 
               client_addr,
               pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND datname = current_database()
          AND application_name = '%s_%s'
        """ % (app_name, packet)
    ):
        print('%s     %s      %s      %s' % (rec[0], rec[1], rec[2], rec[3]))
        found_conns = True
    if not found_conns:
        print('No connections')
    print("============================================")


class DBCGlobal:
    sys_conf = None
    logger = None
    args = None
    db_conns = {}
    is_terminate = False

    errors_count = 0

    def __init__(self):
        try:
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
                "--skip-step-errors",
                help="Skip whole step on first error like Deadlock, QueryCanceledError",
                action='store_true',
                default=False
            )
            parser.add_argument(
                "--skip-action-errors",
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

            try:
                self.args = parser.parse_args()
            except SystemExit:
                sys.exit(0)
            except:
                print(exception_helper())
                sys.exit(0)

            if not len(sys.argv) > 1:
                print("No arguments. Type -h for help.")
                sys.exit(0)

            if self.args.version:
                print("Version %s" % VERSION)
                sys.exit(0)

            self.sys_conf = SysConf()

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
            self.logger = PSCLogger(
                # self.sys_conf.application_name + "_" + self.args.db_name + "_" + packet_name,
                "dbc_" + log_name_part + "_" + self.args.packet_name,
                log_level=logging._nameToLevel[self.sys_conf.log_level.upper()],
                max_bytes=1024*1000*self.sys_conf.file_maxmbytes,
                backup_count=self.sys_conf.file_backupcount,
                delay=1
            )
            self.logger.start()

            AppFileLock(            # AppFileLock used for multiple processes
                self.sys_conf.current_dir if self.sys_conf.lock_file_dir is None else self.sys_conf.lock_file_dir,
                self.sys_conf.application_name + "_" + self.args.packet_name
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


def resultset_hook(ctx, results):
    try:
        if "hook" in ctx.meta_data_json and len(results) > 0:
            if ctx.meta_data_json["hook"]["type"] == "matterhook" and \
                    hasattr(DBC, 'matterhooks') and DBC.matterhooks is not None:
                msg = "#### :gear: %s: %s `->` %s\n" % (ctx.db_name, ctx.packet_name, ctx.step[0])
                if "message" in ctx.meta_data_json["hook"]:
                    msg += ctx.meta_data_json["hook"]["message"]

                if "show_parameters" in ctx.meta_data_json["hook"] and \
                        ctx.meta_data_json["hook"]["show_parameters"] in ("true", "True", "1"):
                    msg += "\n #### Parameters: \n"
                    msg += "```\n"
                    for arg in vars(DBC.args):
                        msg += '%s = %s\n' % (arg, getattr(DBC.args, arg))
                    msg += "```"

                any_item = False
                for result in results:
                    if isinstance(result, tuple):                                   # verbosity = stm_result
                        if "stm_result" in ctx.meta_data_json["hook"]["verbosity"] or \
                                "all" in ctx.meta_data_json["hook"]["verbosity"]:
                            msg += "\n```\n" + str(result) + "\n```"
                            any_item = True
                    if isinstance(result, list) and len(result) > 0 and \
                            result[0] in DBC.sys_conf.plsql_raises:                 # verbosity = raise
                        if "raise" in ctx.meta_data_json["hook"]["verbosity"] or \
                                "all" in ctx.meta_data_json["hook"]["verbosity"]:
                            msg += "\n```\n%s: %s\n```" % (result[0], result[1])
                            any_item = True
                    if isinstance(result, list) and len(result) > 0 and \
                            result[0] not in DBC.sys_conf.plsql_raises:             # verbosity = resultset
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
                    DBC.matterhooks[ctx.meta_data_json["hook"]["channel"]].send(
                        msg,
                        channel=ctx.meta_data_json["hook"]["channel"],
                        username=ctx.meta_data_json["hook"]["username"]
                        if "username" in ctx.meta_data_json["hook"] else "db_converter"
                    )
    except:
        exception_descr = exception_helper(DBC.sys_conf.detailed_traceback)
        DBC.logger.log('%s: Exception in "resultset_hook" %s' % (ctx.info, exception_descr), "Error", do_print=True)


def is_maint_query(query):
    for op in DBC.sys_conf.maint_ops:
        if re.search(r"\b" + re.escape(op) + r"\b", query):
            return True
    return False


def execute_q(ctx, conn, query, isolation_level="READ COMMITTED", read_only=False):
    results = []

    if "client_min_messages" in ctx.meta_data_json:
        # conn.execute("set client_min_messages = 'NOTICE'")
        conn.settings['client_min_messages'] = ctx.meta_data_json["client_min_messages"]

    def filter_notices(msg, msgs_list):
        if msg.details['severity'] in DBC.sys_conf.plsql_raises:
            msgs_list.append([msg.details['severity'], msg.message])
            DBC.logger.log('%s: %s' % (msg.details['severity'], msg.message), "Info", do_print=True)
            return True

    conn.msghook = partial(filter_notices, msgs_list=results)

    try:
        if is_maint_query(query.lower()):
            DBC.logger.log("%s Executing as maintenance query:\n%s" % (ctx.info(), query), "Info", do_print=True)
            conn.execute(query)
        else:
            with conn.xact(isolation=isolation_level) as xact:
                # psc.postgresql.exceptions.ReadOnlyTransactionError: cannot execute ... in a read-only transaction
                if read_only:
                    conn.execute("SET TRANSACTION READ ONLY")

                stms = sqlparse.split(query)
                for stm in stms:
                    prepared = conn.prepare(stm)
                    res = prepared()
                    results.append(res)
                    # ===============================================================================
                    # output to stdout
                    if isinstance(res, tuple):
                        DBC.logger.log('%s' % str(res), "Info", do_print=True)
                    if isinstance(res, list) and len(res) > 0 and res[0] not in DBC.sys_conf.plsql_raises:
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
                        DBC.logger.log('\n%s' % str(table_text), "Info", do_print=True)
                    # ===============================================================================

                if ctx.meta_data_json["type"] == PacketType.NO_COMMIT.value:
                    DBC.logger.log("%s: Performing rollback..." % (ctx.info()), "Info")
                    xact.rollback()
    except postgresql.exceptions.OperationError:
        DBC.logger.log("%s: Transaction aborted" % (ctx.info()), "Info", do_print=True)

    # output via hook
    resultset_hook(ctx, results)


def execute_step(
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
                    DBC.logger.log(
                        "%s: not found generator for this step, but GEN_OBJ_FLD_ is exists" % (ctx.info()),
                        "Error"
                    )
                    raise Exception(msg)
                if ctx.step[0] not in gen_nsp_data:
                    DBC.logger.log(
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
                            DBC.logger.log(
                                "%s: action already executed with hash %s" % (ctx.info(), step_hash),
                                "Info"
                            )
                        else:
                            # ========================================================================
                            if DBC.sys_conf.log_sql == 1:
                                DBC.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")
                            if DBC.sys_conf.execute_sql:
                                if enable_at:
                                    ActionTracker.begin_action(
                                        db_local, ctx.packet_name, ctx.packet_hash, ctx.step[0], ctx.meta_data
                                    )
                                execute_q(ctx, db_local, gen_query)
                                if enable_at:
                                    ActionTracker.apply_action(
                                        db_local, ctx.packet_name, ctx.step[0], step_hash
                                    )
                                steps_hashes[step_hash] = ctx.step[0]
                                DBC.logger.log("%s: action finished" % (ctx.info()), "Info")

                            if gen_nsp_i[0] is not None and len(str(gen_nsp_i[0])) > 0:  # run maintenance command
                                if DBC.sys_conf.log_sql == 1:
                                    DBC.logger.log("%s:\n%s" % (ctx.info(), gen_nsp_i[0]), "Info")
                                if DBC.sys_conf.execute_sql:
                                    execute_q(ctx, db_local, gen_nsp_i[0])

                            if gen_obj_i[0] is not None and len(str(gen_obj_i[0])) > 0:  # run maintenance command
                                if DBC.sys_conf.log_sql == 1:
                                    DBC.logger.log("%s:\n%s" % (ctx.info(), gen_obj_i[0]), "Info")
                                if DBC.sys_conf.execute_sql:
                                    execute_q(ctx, db_local, gen_obj_i[0])
                            # ========================================================================
            # case 2: only OBJ generator is exists
            if ctx.step[1].find("GEN_NSP_FLD_") == -1 and ctx.step[1].find("GEN_OBJ_FLD_") > -1:
                if ctx.step[0] not in gen_obj_data:
                    msg = "%s: not found generator for this step, but GEN_OBJ_FLD_ is exists" % (ctx.info())
                    DBC.logger.log(msg, "Error")
                    raise Exception(msg)
                for gen_obj_i in gen_obj_data[ctx.step[0]]:
                    gen_query = parse_query_placeholder(ctx.step[1], gen_obj_i, 'GEN_OBJ_FLD_')
                    step_hash = hashlib.md5(gen_query.encode()).hexdigest()
                    if step_hash in steps_hashes:
                        continue
                    if enable_at and ActionTracker.is_action_exists(db_local, ctx.packet_name, ctx.step[0], step_hash):
                        steps_hashes[step_hash] = ctx.step[0]
                        DBC.logger.log(
                            "%s: already executed with hash %s" % (ctx.info(), step_hash),
                            "Info"
                        )
                    else:
                        # ========================================================================
                        if DBC.sys_conf.log_sql == 1:
                            DBC.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")
                        if DBC.sys_conf.execute_sql:
                            if enable_at: ActionTracker.begin_action(
                                db_local, ctx.packet_name, ctx.packet_hash, ctx.step[0], ctx.meta_data
                            )
                            execute_q(ctx, db_local, gen_query)
                            if enable_at: ActionTracker.apply_action(db_local, ctx.packet_name, ctx.step[0], step_hash)
                            steps_hashes[step_hash] = ctx.step[0]
                            DBC.logger.log("%s: action finished" % (ctx.info()), "Info")

                        if gen_obj_i[0] is not None and len(str(gen_obj_i[0])) > 0:  # run maintenance command
                            if DBC.sys_conf.log_sql == 1:
                                DBC.logger.log("%s:\n%s" % (ctx.info(), gen_obj_i[0]), "Info")
                            if DBC.sys_conf.execute_sql:
                                execute_q(ctx, db_local, gen_obj_i[0])

                        # ========================================================================
            # case 3: only NSP generator is exists
            if ctx.step[1].find("GEN_NSP_FLD_") > -1 and ctx.step[1].find("GEN_OBJ_FLD_") == -1:
                if ctx.step[0] not in gen_nsp_data:
                    msg = "%s: not found generator for this step, but GEN_NSP_FLD_ is exists" % (ctx.info())
                    DBC.logger.log(msg, "Error")
                    raise Exception(msg)
                for gen_nsp_i in gen_nsp_data[ctx.step[0]]:
                    gen_query = parse_query_placeholder(ctx.step[1], gen_nsp_i, 'GEN_NSP_FLD_')
                    step_hash = hashlib.md5(gen_query.encode()).hexdigest()
                    if step_hash in steps_hashes:
                        continue
                    if enable_at and ActionTracker.is_action_exists(db_local, ctx.packet_name, ctx.step[0], step_hash):
                        steps_hashes[step_hash] = ctx.step[0]
                        DBC.logger.log("%s: action already executed with hash %s" % (ctx.info(), step_hash), "Info")
                    else:
                        # ========================================================================
                        if DBC.sys_conf.log_sql == 1:
                            DBC.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")
                        if DBC.sys_conf.execute_sql:
                            if enable_at: ActionTracker.begin_action(
                                db_local, ctx.packet_name, ctx.packet_hash, ctx.step[0], ctx.meta_data
                            )
                            execute_q(ctx, db_local, gen_query)
                            if enable_at: ActionTracker.apply_action(db_local, ctx.packet_name, ctx.step[0], step_hash)
                            steps_hashes[step_hash] = ctx.step[0]
                            DBC.logger.log("%s: action finished" % (ctx.info()), "Info")

                        if gen_nsp_i[0] is not None and len(str(gen_nsp_i[0])) > 0:  # run maintenance command
                            if DBC.sys_conf.log_sql == 1:
                                DBC.logger.log("%s:\n%s" % (ctx.info(), gen_nsp_i[0]), "Info")
                            if DBC.sys_conf.execute_sql:
                                execute_q(ctx, db_local, gen_nsp_i[0])
                        # ========================================================================
            # case 4: no generators
            if ctx.step[1].find("GEN_NSP_FLD_") == -1 and ctx.step[1].find("GEN_OBJ_FLD_") == -1:
                step_hash = hashlib.md5(ctx.step[1].encode()).hexdigest()
                if step_hash not in steps_hashes:
                    if enable_at and ActionTracker.is_action_exists(db_local, ctx.packet_name, ctx.step[0], step_hash):
                        steps_hashes[step_hash] = ctx.step[0]
                        DBC.logger.log(
                            "%s: action already executed with hash %s" % (ctx.info(), step_hash),
                            "Info"
                        )
                    else:
                        # ========================================================================
                        if DBC.sys_conf.log_sql == 1:
                            DBC.logger.log("%s:\n%s" % (ctx.info(), ctx.step[1]), "Info")
                        if DBC.sys_conf.execute_sql:
                            if enable_at: ActionTracker.begin_action(
                                db_local, ctx.packet_name, ctx.packet_hash, ctx.step[0], ctx.meta_data
                            )
                            execute_q(ctx, db_local, ctx.step[1])
                            if enable_at:
                                ActionTracker.apply_action(db_local, ctx.packet_name, ctx.step[0], step_hash)
                            steps_hashes[step_hash] = ctx.step[0]
                            DBC.logger.log("%s: action finished" % (ctx.info()), "Info")
                        # ========================================================================
        except (
            postgresql.exceptions.PLPGSQLRaiseError
        ):
            raise_error_logic(ctx)
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
            if DBC.is_terminate:
                return 'done', None
            DBC.logger.log(
                'Exception in %s (execute_step): %s. Reconnecting after %d sec...' %
                (ctx.info(), str(e), DBC.sys_conf.conn_exception_sleep_interval),
                "Error",
                do_print=True
            )
            time.sleep(DBC.sys_conf.conn_exception_sleep_interval)
            if DBC.args.skip_step_errors:
                return 'exception', 'skip_step'
            elif DBC.args.skip_action_errors:
                steps_hashes[step_hash] = ctx.step[0]
                DBC.logger.log(
                    '%s (execute_step): action %s in step %s skipped!' %
                    (ctx.info(), step_hash, ctx.step[0]),
                    "Error",
                    do_print=True
                )
                DBC.errors_count += 1
                execute_step_do_work = True
            elif e.code == '40P01':
                return 'exception', 'deadlock_detected'
            else:
                return 'exception', 'connection'
        except:
            exception_descr = exception_helper(DBC.sys_conf.detailed_traceback)
            DBC.logger.log(
                'Exception in "execute_step" %s: \n%s' % (ctx.info(), exception_descr),
                "Error",
                do_print=True
            )
            return 'exception', exception_descr

    return 'done', None


def raise_error_logic(ctx):
    try:
        if "hook" in ctx.meta_data_json:
            if ctx.meta_data_json["hook"]["type"] == "matterhook" and DBC.matterhooks is not None:
                msg = "#### :comet: %s: %s `->` %s\n" % (ctx.db_name, ctx.packet_name, ctx.step[0])
                msg += ctx.meta_data_json["hook"]["message"]
                exc_type, exc_value, _ = sys.exc_info()
                msg += "\n``` bash\n" + str(exc_value) + "\n```"

                if "show_parameters" in ctx.meta_data_json["hook"] and \
                        ctx.meta_data_json["hook"]["show_parameters"] in ("true", "True", "1"):
                    msg += "\n #### Parameters: \n"
                    msg += "```\n"
                    for arg in vars(DBC.args):
                        msg += '%s = %s\n' % (arg, getattr(DBC.args, arg))
                    msg += "```"

                DBC.matterhooks[ctx.meta_data_json["hook"]["channel"]].send(
                    msg,
                    channel=ctx.meta_data_json["hook"]["channel"],
                    username=ctx.meta_data_json["hook"]["username"]
                    if "username" in ctx.meta_data_json["hook"] else "db_converter"
                )
    except:
        exception_descr = exception_helper(DBC.sys_conf.detailed_traceback)
        DBC.logger.log(
            'Exception in "raise_error_logic" %s: \n%s' % (ctx.info(), exception_descr),
            "Error",
            do_print=True
        )


def execute_ro_step(
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
        return execute_q(ctx, conn, query, isolation_level='REPEATABLE READ', read_only=True)

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
                        if DBC.sys_conf.log_sql == 1:
                            DBC.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")
                        if DBC.sys_conf.execute_sql:
                            execute_ro(db_local, gen_query)
                            steps_hashes[step_hash] = ctx.step[0]
                            DBC.logger.log("%s: action finished" % (ctx.info()), "Info")

                        if gen_nsp_i[0] is not None and len(str(gen_nsp_i[0])) > 0:  # run maintenance command
                            if DBC.sys_conf.log_sql == 1:
                                DBC.logger.log("%s:\n%s" % (ctx.info(), gen_nsp_i[0]), "Info")

                            if DBC.sys_conf.execute_sql:
                                execute_ro(db_local, gen_nsp_i[0])

                        if gen_obj_i[0] is not None and len(str(gen_obj_i[0])) > 0:  # run maintenance command
                            if DBC.sys_conf.log_sql == 1:
                                DBC.logger.log("%s:\n%s" % (ctx.info(), gen_obj_i[0]), "Info")

                            if DBC.sys_conf.execute_sql:
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
                    if DBC.sys_conf.log_sql == 1:
                        DBC.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")

                    if DBC.sys_conf.execute_sql:
                        execute_ro(db_local, gen_query)
                        steps_hashes[step_hash] = ctx.step[0]
                        DBC.logger.log("%s: action finished" % (ctx.info()), "Info")

                    if gen_obj_i[0] is not None and len(str(gen_obj_i[0])) > 0:  # run maintenance command
                        if DBC.sys_conf.log_sql == 1:
                            DBC.logger.log("%s:\n%s" % (ctx.info(), gen_obj_i[0]), "Info")
                        if DBC.sys_conf.execute_sql:
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
                    if DBC.sys_conf.log_sql == 1:
                        DBC.logger.log("%s:\n%s" % (ctx.info(), gen_query), "Info")

                    if DBC.sys_conf.execute_sql:
                        execute_ro(db_local, gen_query)
                        steps_hashes[step_hash] = ctx.step[0]
                        DBC.logger.log("%s: action finished" % (ctx.info()), "Info")

                    if gen_nsp_i[0] is not None and len(str(gen_nsp_i[0])) > 0:  # run maintenance command
                        if DBC.sys_conf.log_sql == 1:
                            DBC.logger.log("%s:\n%s" % (ctx.info(), gen_nsp_i[0]), "Info")

                        if DBC.sys_conf.execute_sql:
                            execute_ro(db_local, gen_nsp_i[0])
                    # ========================================================================
            # case 4: no generators
            if ctx.step[1].find("GEN_NSP_FLD_") == -1 and ctx.step[1].find("GEN_OBJ_FLD_") == -1:
                step_hash = hashlib.md5(ctx.step[1].encode()).hexdigest()
                if step_hash not in steps_hashes:
                        # ========================================================================
                        if DBC.sys_conf.log_sql == 1:
                            DBC.logger.log("%s:\n%s" % (ctx.info(), ctx.step[1]), "Info")

                        if DBC.sys_conf.execute_sql:
                            execute_ro(db_local, ctx.step[1])
                            steps_hashes[step_hash] = ctx.step[0]
                            DBC.logger.log("%s: action finished" % (ctx.info()), "Info")
                        # ========================================================================
        except (
                postgresql.exceptions.PLPGSQLRaiseError,
                postgresql.exceptions.ReadOnlyTransactionError
        ):
            raise_error_logic(ctx)
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
            DBC.logger.log(
                'Exception in %s (execute_ro_step): %s. Reconnecting after %d sec...' %
                (ctx.info(), str(e), DBC.sys_conf.conn_exception_sleep_interval),
                "Error",
                do_print=True
            )
            time.sleep(DBC.sys_conf.conn_exception_sleep_interval)
            if DBC.args.skip_step_errors:
                return 'exception', 'skip_step'
            elif DBC.args.skip_action_errors:
                steps_hashes[step_hash] = ctx.step[0]
                DBC.logger.log(
                    '%s (execute_ro_step): action %s in step %s skipped!' %
                    (ctx.info(), step_hash, ctx.step[0]),
                    "Error",
                    do_print=True
                )
                DBC.errors_count += 1
                execute_step_do_work = True
            elif e.code == '40P01':
                return 'exception', 'deadlock_detected'
            else:
                return 'exception', 'connection'
        except:
            exception_descr = exception_helper(DBC.sys_conf.detailed_traceback)
            DBC.logger.log(
                'Exception in %s "execute_ro_step": \n%s' % (ctx.info(), exception_descr),
                "Error",
                do_print=True
            )
            return 'exception', exception_descr

    return 'done', None


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


class DBCLocal:
    lock = threading.Lock()
    workers_db_pid = {}     # key is "db_name", value is array of pids
    worker_threads = {}     # key is "db_name", value is array of threads (one lock_observer and one worker_db_func)
    db_name = None
    workers_status = {}     # key is "db_name", boolean value: True is active, False is finished

    def __init__(self, db_name):
        self.db_name = db_name

    def get_pids(self):
        try:
            if self.db_name in self.workers_db_pid:
                yield next(iter(self.workers_db_pid[self.db_name]))
            else:
                return next(iter([]))
        except StopIteration:
            return

    def remove_pid(self, pid):
        workers_db_pids = [v for v in self.get_pids()]
        workers_db_pids.remove(pid)
        self.workers_db_pid[self.db_name] = workers_db_pids

    def append_pid(self, pid):
        self.workers_db_pid.setdefault(self.db_name, []).append(pid)

    def append_thread(self, thread):
        self.worker_threads.setdefault(self.db_name, []).append(thread)

    def get_threads(self):
        return self.worker_threads[self.db_name]

    def get_num_observed_threads_(self):
        return self.worker_threads[self.db_name].num_observed_threads

    def get_worker_status(self):
        if self.db_name in self.workers_status:
            return self.workers_status[self.db_name]
        else:
            return None

    def set_worker_status_start(self):
        self.workers_status[self.db_name] = True

    def set_worker_status_finish(self):
        self.workers_status[self.db_name] = False

    @threaded
    def lock_observer(self, thread_name, str_conn, app_name_postfix):
        def sleep_lo():
            for i in range(50):
                if self.get_worker_status() is True or self.get_worker_status() is None:
                    time.sleep(DBC.sys_conf.lock_observer_sleep_interval/50)
                    if DBC.is_terminate:
                        break
                if self.get_worker_status() is False:
                    break

        sleep_lo()

        DBC.logger.log(
            'Thread \'%s\' runned! Observed pids: %s' % (thread_name, str([v for v in self.get_pids()])),
            "Info"
        )
        do_work = True
        db_conn = None
        while do_work:
            do_work = False
            try:
                db_conn = postgresql.open(str_conn)
                app_name = DBC.sys_conf.application_name + "_" + app_name_postfix
                db_conn.execute("SET application_name = '%s'" % app_name)

                while len([thread for thread in self.get_threads() if thread.is_alive()]) > 1:
                    for pid in self.get_pids():
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
                            )""" % (DBC.sys_conf.cancel_blocker_tx_timeout, pid)
                        )
                        if pid_is_locker:
                            db_conn.execute("SELECT pg_cancel_backend(%d)" % pid)
                            self.lock.acquire()
                            if pid in self.get_pids(): self.remove_pid(pid)
                            self.lock.release()
                            DBC.logger.log('%s: stopped pid %d as blocker' % (thread_name, pid), "Info")
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
                        """ % (pid, DBC.sys_conf.cancel_wait_tx_timeout))
                        if any_heavyweight_lock_already:
                            db_conn.execute("SELECT pg_cancel_backend(%d)" % pid)
                            self.lock.acquire()
                            if pid in self.get_pids(): self.remove_pid(pid)
                            self.lock.release()
                            DBC.logger.log('%s: stopped pid %d with heavyweight lock' % (thread_name, pid), "Info")
                        # ===========================================================================
                    DBC.logger.log(
                        '%s: iteration done. Sleep on %d seconds...' %
                        (thread_name, DBC.sys_conf.lock_observer_sleep_interval),
                        "Info"
                    )
                    sleep_lo()
            except (
                    postgresql.exceptions.QueryCanceledError,
                    postgresql.exceptions.AdminShutdownError,
                    postgresql.exceptions.CrashShutdownError,
                    postgresql.exceptions.ServerNotReadyError,
                    AttributeError  # AttributeError: 'Statement' object has no attribute '_row_constructor'
            ) as e:
                if DBC.is_terminate:
                    return
                do_work = True
                DBC.logger.log(
                    'Exception in \'%s\': %s. Reconnecting after %d sec...' %
                    (thread_name, str(e), DBC.sys_conf.conn_exception_sleep_interval),
                    "Error"
                )
                time.sleep(DBC.sys_conf.conn_exception_sleep_interval)
            except (
                postgresql.exceptions.AuthenticationSpecificationError,
                postgresql.exceptions.ClientCannotConnectError,
                TimeoutError
            ) as e:
                DBC.logger.log(
                    'Exception in %s: \n%s' % (thread_name, exception_helper(DBC.sys_conf.detailed_traceback)),
                    "Error",
                    do_print=True
                )
            finally:
                if db_conn is not None:
                    db_conn.close()
                DBC.logger.log('Thread %s finished!' % thread_name, "Info")

    def parse_packet(self, packet_name, thread_name):
        step_files = []
        gen_obj_files = {}
        gen_nsp_files = {}
        # parse packet
        try:
            packet_full_content = []
            meta_data = None
            packet_dir = os.path.join(DBC.sys_conf.current_dir, 'packets', packet_name)
            for step in os.listdir(packet_dir):
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
                meta_data_json["tags"] = tags
            # ========================================================================

            step_files = sorted(step_files, key=lambda val: val[0])
            packet_full_content = sorted(packet_full_content, key=lambda val: val[0])
            packet_full_content_res = ""
            for v in packet_full_content:
                packet_full_content_res += v
            packet_hash = hashlib.md5(packet_full_content_res.encode()).hexdigest()
        except:
            DBC.logger.log(
                'Exception in \'%s\' (parse_packet): \n%s' %
                    (thread_name, exception_helper(DBC.sys_conf.detailed_traceback)),
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
            return getattr(DBC.sys_conf, name)

        is_super = get_scalar(db_local, 'select usesuper from pg_user where usename = CURRENT_USER')
        try:
            if DBC.sys_conf.log_level == "Debug":
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
            DBC.logger.log(
                'Exception in prepare_session: %s. Skip configuring session variables...' % str(e),
                "Error"
            )

    @threaded
    def ro_worker_db_func(self, thread_name, db_conn_str, db_name, packet_name):
        DBC.logger.log("Started '%s' thread for '%s' database" % (thread_name, db_name), "Info")
        self.set_worker_status_start()

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

        while do_work:
            do_work = False
            try:
                # check conn to DB
                if db_local is not None:    # connection db_local has been already established
                    DBC.logger.log("%s: try 'SELECT 1'" % thread_name, "Info")
                    try:
                        db_local.execute("SELECT 1")    # live?
                        if current_pid not in self.get_pids():
                            self.append_pid(current_pid)
                    except:
                        DBC.logger.log("%s: Connection to DB is broken" % thread_name, "Error")
                        db_local = None     # needs reconnect

                # ======================================================
                # connecting to DB, session variables initialization
                if db_local is None:
                    if current_pid is not None and current_pid in self.get_pids():
                        self.remove_pid(current_pid)

                    DBC.logger.log("Thread '%s': connecting to '%s' database..." % (thread_name, db_name), "Info")
                    db_local = postgresql.open(db_conn_str)
                    db_local.execute(
                        "SET application_name = '%s'" %
                        (DBC.sys_conf.application_name + "_" + os.path.splitext(packet_name)[0])
                    )

                    self.prepare_session(db_local, meta_data_json)

                    current_pid = get_scalar(db_local, "SELECT pg_backend_pid()")
                    DBC.db_conns[current_pid] = db_local
                    self.append_pid(current_pid)
                    DBC.logger.log(
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
                        DBC.logger.log(
                            '%s: progress %s' % (ctx.info(), progress),
                            "Info",
                            do_print=True
                        )
                        result, exception_descr = execute_ro_step(
                            ctx,
                            db_local,
                            gen_nsp_data,
                            gen_obj_data,
                            steps_hashes
                        )
                        if result == 'exception' and exception_descr == 'connection':
                            # transaction cancelled or connection stopped
                            time.sleep(DBC.sys_conf.conn_exception_sleep_interval)
                            return None, None, True
                        if result == 'exception' and exception_descr == 'skip_step':
                            DBC.logger.log(
                                'Thread \'%s\' (steps_processing): step %s skipped!' %
                                (thread_name, step[0]),
                                "Error",
                                do_print=True
                            )
                        if result == 'exception' and exception_descr is not None and \
                                exception_descr not in('connection', 'skip'):
                            return result, exception_descr, False
                    return None, None, False

                # ===========================================================================
                # read only steps processing
                gen_obj_data = {}
                gen_nsp_data = {}

                for step, query in gen_obj_files.items():
                    gen_obj_data[step.replace("_gen_obj", "_step")] = get_resultset(db_local, query)
                for step, query in gen_nsp_files.items():
                    gen_nsp_data[step.replace("_gen_nsp", "_step")] = get_resultset(db_local, query)

                _, exception_descr, do_work = ro_steps_processing()
                # ===========================================================================
            except (
                    postgresql.exceptions.QueryCanceledError,
                    postgresql.exceptions.AdminShutdownError,
                    postgresql.exceptions.CrashShutdownError,
                    postgresql.exceptions.ServerNotReadyError,
                    postgresql.exceptions.DeadlockError,
                    AttributeError  # AttributeError: 'Statement' object has no attribute '_row_constructor'
            ) as e:
                if DBC.is_terminate:
                    return
                do_work = True
                DBC.logger.log(
                    'Exception in \'%s\': %s. Reconnecting after %d sec...' %
                    (thread_name, str(e), DBC.sys_conf.conn_exception_sleep_interval),
                    "Error"
                )
            except:
                do_work = False
                exception_descr = exception_helper(DBC.sys_conf.detailed_traceback)
                msg = 'Exception in \'%s\' %s on processing packet \'%s\': \n%s' % \
                      (thread_name, str(current_pid), packet_name, exception_descr)
                DBC.logger.log(msg, "Error", do_print=True)

        self.lock.acquire()
        if current_pid is not None and current_pid in self.get_pids():
            self.remove_pid(current_pid)
        self.lock.release()

        DBC.logger.log("Finished '%s' thread for '%s' database" % (thread_name, db_name), "Info")
        if exception_descr is None:
            DBC.logger.log(
                '<-------- Packet \'%s\' finished for \'%s\' database!' % \
                    (DBC.args.packet_name, db_name),
                "Info",
                do_print=True
            )
        else:
            DBC.logger.log(
                '<-------- Packet \'%s\' failed for \'%s\' database!' % \
                    (DBC.args.packet_name, db_name),
                "Error",
                do_print=True
            )
        self.set_worker_status_finish()

    @threaded
    def worker_db_func(self, thread_name, db_conn_str, db_name, packet_name):
        DBC.logger.log("Started '%s' thread for '%s' database" % (thread_name, db_name), "Info")
        self.set_worker_status_start()

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

        while do_work:
            do_work = False
            try:
                # check conn to DB
                if db_local is not None:    # connection db_local has been already established
                    DBC.logger.log("%s: try 'SELECT 1'" % thread_name, "Info")
                    try:
                        db_local.execute("SELECT 1")    # live?
                        if current_pid not in self.get_pids():
                            self.append_pid(current_pid)
                    except:
                        DBC.logger.log("%s: Connection to DB is broken" % thread_name, "Error")
                        db_local = None     # needs reconnect

                # ======================================================
                # connecting to DB, session variables initialization
                if db_local is None:
                    if current_pid is not None and current_pid in self.get_pids():
                        self.remove_pid(current_pid)

                    DBC.logger.log("Thread '%s': connecting to '%s' database..." % (thread_name, db_name), "Info")
                    db_local = postgresql.open(db_conn_str)
                    db_local.execute(
                        "SET application_name = '%s'" %
                        (DBC.sys_conf.application_name + "_" + os.path.splitext(packet_name)[0])
                    )

                    self.prepare_session(db_local, meta_data_json)

                    current_pid = get_scalar(db_local, "SELECT pg_backend_pid()")
                    self.append_pid(current_pid)
                    DBC.logger.log(
                        "Thread '%s': connected to '%s' database with pid %d" %
                        (thread_name, db_name, current_pid),
                        "Info"
                    )
                # ======================================================
                if not DBC.args.force:
                    packet_status = ActionTracker.get_packet_status(db_local, packet_name)
                    if "hash" in packet_status:
                        if packet_status["hash"] != packet_hash:
                            do_work = False
                            DBC.logger.log(
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
                            DBC.logger.log(
                                '%s: progress %s' % (ctx.info(), progress),
                                "Info",
                                do_print=True
                            )
                            result, exception_descr = execute_step(
                                ctx,
                                db_local,
                                gen_nsp_data,
                                gen_obj_data,
                                steps_hashes
                            )
                            if result == 'exception' and exception_descr == 'connection':
                                # transaction cancelled or connection stopped
                                time.sleep(DBC.sys_conf.conn_exception_sleep_interval)
                                return None, None, True
                            if result == 'exception' and exception_descr == 'skip_step':
                                DBC.logger.log(
                                    'Thread \'%s\' (steps_processing): step %s skipped!' %
                                    (thread_name, step[0]),
                                    "Error",
                                    do_print=True
                                )
                            if result == 'done' and exception_descr is None:
                                # step successfully complete
                                ActionTracker.set_step_status(db_local, packet_name, step[0], result)
                            if result == 'exception' and exception_descr is not None and \
                                    exception_descr not in('connection', 'skip'):
                                # syntax exception or pre/post check raised exception
                                ActionTracker.set_step_exception_status(db_local, packet_name, step[0], exception_descr)
                                return result, exception_descr, False
                    return None, None, False

                # ===========================================================================
                # steps processing
                gen_obj_data = {}
                gen_nsp_data = {}

                _, exception_descr, do_work = steps_processing(run_once=True)

                for step, query in gen_obj_files.items():
                    gen_obj_data[step.replace("_gen_obj", "_step")] = get_resultset(db_local, query)
                for step, query in gen_nsp_files.items():
                    gen_nsp_data[step.replace("_gen_nsp", "_step")] = get_resultset(db_local, query)

                _, exception_descr, do_work = steps_processing()
                # ===========================================================================
            except (
                    postgresql.exceptions.QueryCanceledError,
                    postgresql.exceptions.AdminShutdownError,
                    postgresql.exceptions.CrashShutdownError,
                    postgresql.exceptions.ServerNotReadyError,
                    postgresql.exceptions.DeadlockError,
                    AttributeError  # AttributeError: 'Statement' object has no attribute '_row_constructor'
            ) as e:
                if DBC.is_terminate:
                    return
                do_work = True
                DBC.logger.log(
                    'Exception in \'%s\': %s. Reconnecting after %d sec...' %
                    (thread_name, str(e), DBC.sys_conf.conn_exception_sleep_interval),
                    "Error"
                )
            except (
                postgresql.exceptions.AuthenticationSpecificationError,
                postgresql.exceptions.ClientCannotConnectError
            ) as e:
                DBC.logger.log(
                    'Exception in %s: \n%s' % (thread_name, exception_helper(DBC.sys_conf.detailed_traceback)),
                    "Error"
                )
            except:
                do_work = False
                exception_descr = exception_helper(DBC.sys_conf.detailed_traceback)
                msg = 'Exception in \'%s\' %d on processing packet \'%s\': \n%s' % \
                      (thread_name, current_pid, packet_name, exception_descr)
                DBC.logger.log(msg, "Error", do_print=True)

        if not work_breaked and DBC.errors_count == 0:
            ActionTracker.set_packet_status(db_local, packet_name, 'done' if exception_descr is None else 'exception')

        if not work_breaked and DBC.errors_count > 0:
            ActionTracker.set_packet_status(db_local, packet_name, 'exception')

        self.lock.acquire()
        if current_pid is not None and current_pid in self.get_pids():
            self.remove_pid(current_pid)
        self.lock.release()

        DBC.logger.log("Finished '%s' thread for '%s' database" % (thread_name, db_name), "Info")
        if exception_descr is None:
            DBC.logger.log(
                '<-------- Packet \'%s\' finished for \'%s\' database!' % \
                    (DBC.args.packet_name, db_name),
                "Info",
                do_print=True
            )
        else:
            DBC.logger.log(
                '<-------- Packet \'%s\' failed for \'%s\' database!' % \
                    (DBC.args.packet_name, db_name),
                "Error",
                do_print=True
            )
        self.set_worker_status_finish()


if __name__ == "__main__":
    DBC = DBCGlobal()           # global object with configuration and logger
    DBCL_ALL = {}               # dict with all converter objects:
                                # each object contains two threads: lock_observer and worker_db

    DBC.logger.log('=====> DBC %s started' % VERSION, "Info", do_print=True)

    # processing specific DB
    def run_on_db(db_name, str_conn):
        global DBC
        global DBCL_ALL

        meta_data_file_name = os.path.join(
            DBC.sys_conf.current_dir,
            "packets",
            DBC.args.packet_name,
            "meta_data.json"
        )

        packet_type = None
        if os.path.isfile(meta_data_file_name):
            current_file = open(meta_data_file_name, 'r')
            file_content = current_file.read()
            current_file.close()
            meta_data_json = json.loads(file_content)
            if "type" in meta_data_json:
                packet_type = meta_data_json["type"]
            else:
                packet_type = PacketType.DEFAULT.value
        else:
            packet_type = PacketType.DEFAULT.value

        packet_status = {}
        if packet_type == PacketType.DEFAULT.value:
            ActionTracker.init_tbls(str_conn)
            db_conn = postgresql.open(str_conn)
            packet_status = ActionTracker.get_packet_status(db_conn, DBC.args.packet_name)
            db_conn.close()

        if DBC.args.stop:
            db_conn = postgresql.open(str_conn)
            terminate_conns(db_conn, db_name, DBC.sys_conf.application_name, DBC.args.packet_name)
            db_conn.close()
        elif DBC.args.wipe:
            db_conn = postgresql.open(str_conn)
            wipe_res = ActionTracker.wipe_packet(db_conn, DBC.args.packet_name)
            db_conn.close()
            if wipe_res:
                print("=====> Database '%s', packet '%s' successfully wiped!" % (db_name, DBC.args.packet_name))
            else:
                print("=====> Database '%s', packet '%s' data not found!" % (db_name, DBC.args.packet_name))
        elif DBC.args.status:
            print(
                "=====> Database '%s', packet '%s' status: %s" %
                (db_name, DBC.args.packet_name, "new" if "status" not in packet_status else packet_status["status"])
            )
            if "exception_descr" in packet_status and packet_status["exception_descr"] is not None:
                print("       Action date time: %s" % str(packet_status["exception_dt"]))
                print("=".join(['=' * 100]))
                print(packet_status["exception_descr"])
                print("=".join(['=' * 100]))
        else:
            if "status" not in packet_status or ("status" in packet_status and packet_status["status"] != 'done'):
                DBCL = DBCLocal(db_name)

                DBCL.append_thread(
                    DBCL.lock_observer("lock_observer_%s" % str(db_name), str_conn, DBC.args.packet_name)
                )

                if packet_type == PacketType.READ_ONLY.value:
                    DBCL.append_thread(
                        DBCL.ro_worker_db_func(
                            "ro_manager_db_%s" % str(db_name), str_conn, db_name, DBC.args.packet_name
                        )
                    )
                else:
                    DBCL.append_thread(
                        DBCL.worker_db_func(
                            "manager_db_%s" % str(db_name), str_conn, db_name, DBC.args.packet_name
                        )
                    )

                DBCL_ALL[db_name] = DBCL
                DBC.logger.log(
                    '--------> Packet \'%s\' started for \'%s\' database!' % \
                        (DBC.args.packet_name, db_name),
                    "Info",
                    do_print=True
                )

            if "status" in packet_status and packet_status["status"] == 'done':
                DBC.logger.log(
                    '<-------- Packet \'%s\' already deployed to \'%s\' database!' % \
                        (DBC.args.packet_name, db_name),
                    "Info",
                    do_print=True
                )

    # helper for iterate all threads
    def iterate_threads():
        try:
            threads = next(iter([th for _, th in DBCL_ALL.items()])).worker_threads
        except StopIteration:
            return
        common_list_of_threads = []
        for _, threads_per_db in threads.items():
            common_list_of_threads.extend(threads_per_db)
        for thread_i in common_list_of_threads:
            yield thread_i

    # function for synchronous/asynchronous behaviour
    def wait_threads():
        alive_count = 1
        live_iteration = 0
        while alive_count > 0:
            with SignalHandler() as handler:
                alive_count = len([thread for thread in iterate_threads() if thread.is_alive()])
                if alive_count == 0: break
                time.sleep(0.5)
                if live_iteration % (20 * 3) == 0:
                    DBC.logger.log('Live %s threads' % alive_count, "Info")
                live_iteration += 1
                if handler.interrupted:
                    DBC.is_terminate = True
                    DBC.logger.log('Received termination signal!', "Info", do_print=True)
                    for _, conn in DBC.db_conns.items():
                        conn.interrupt()
                    DBC.logger.log('Stopping with AppFileLock...', "Info", do_print=True)
                    handler.unlock()

    # processing of "db_name" parameter
    dbs = []                        # specific list of databases
    if DBC.args.db_name == 'ALL':   # all databases
        for db_name, str_conn in DBC.sys_conf.dbs_dict.items():
            if db_name not in dbs:
                dbs.append(db_name)
    elif DBC.args.db_name.find('ALL,exclude:') == 0:
        all_dbs = [db_name for db_name, _ in DBC.sys_conf.dbs_dict.items()]
        exclude_dbs = []
        not_dbs_param = DBC.args.db_name[len('ALL,exclude:'):].split(",")
        for not_db in not_dbs_param:
            if not_db.find("*") == -1:
                exclude_dbs.append(not_db)
            else:
                for db in all_dbs:
                    if match(not_db, db) and db not in exclude_dbs:
                        exclude_dbs.append(db)
        for db in all_dbs:
            if db not in exclude_dbs and db not in dbs:
                dbs.append(db)
    else:
        dbs_prepare = DBC.args.db_name.split(",")
        for db in dbs_prepare:
            for db_name, _ in DBC.sys_conf.dbs_dict.items():
                if match(db, db_name) and db_name not in dbs:
                    dbs.append(db_name)

    # ========================================================================
    # confirmation
    break_deployment = False
    if not DBC.args.list and not DBC.args.status and DBC.sys_conf.db_name_all_confirmation:
        if len(dbs) > 1 and not DBC.args.force:
            print("Deployment will be performed on these databases:\n")
            for db_name in dbs:
                print("     " + db_name)
            cmd_question = input('\nDo you want to continue? Type YES to continue...\n')
            if cmd_question != "YES":
                print('Stopping...')
                break_deployment = True
    # ========================================================================

    if DBC.args.list:
        print("List of targets:")
        for db_name in dbs:
            print("     " + db_name)
            break_deployment = True

    if not break_deployment:
        if len(dbs) == 0:
            DBC.logger.log('No target databases!', "Error", do_print=True)
        for db_name in dbs:
            try:
                run_on_db(db_name, DBC.sys_conf.dbs_dict[db_name])
                if DBC.args.seq:
                    wait_threads()
            except (
                    postgresql.exceptions.AuthenticationSpecificationError,
                    postgresql.exceptions.ClientCannotConnectError,
                    TimeoutError
            ) as e:
                DBC.logger.log(
                    'Cannot connect to %s: \n%s' % (db_name, exception_helper(DBC.sys_conf.detailed_traceback)),
                    "Error",
                    do_print=True
                )

    if not break_deployment:
        wait_threads()
    PSCLogger.instance().stop()
    DBC.logger.log('<===== DBC %s finished' % VERSION, "Info",  do_print=True)
