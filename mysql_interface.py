import mysql.connector
import dpower_tools
from config import *
import time, os
import logging

'''
Set timezone to be compatible with zabbix server
'''
os.environ['TZ'] = dict_setup["metric_sent_timezone"]
timestamp = int(time.time())
logger = logging.getLogger("PowerCollector")


class MySQLInterface(object):
    def parse_innodb_engine(self, show_innodb_status):
        text2parse = show_innodb_status[2]
        parsed = dict()
        for line in text2parse.split("\n"):
            if "srv_master_thread log flush and writes" in line.split(':'):
                parsed['mysql.innodb[srv_master_thread_log]'] = line.split(':')[1]

            if "OS WAIT ARRAY INFO: reservation count" in line:
                parsed['mysql.innodb[os_wait_reservation_count]'] = dpower_tools.findNumber(line)

            if "OS WAIT ARRAY INFO: signal count" in line:
                parsed['mysql.innodb[os_wait_signal_count]'] = dpower_tools.findNumber(line)

            if "Mutex spin waits" in line:
                splited = line.split(',')
                parsed['mysql.innodb[mutex_spin_waits]'] = dpower_tools.findNumber(splited[0])
                parsed['mysql.innodb[mutex_spin_rounds]'] = dpower_tools.findNumber(splited[1])
                parsed['mysql.innodb[innodb_mutex_spin_os_waits]'] = dpower_tools.findNumber(splited[2])

            if "Total memory allocated" in line:
                splited = line.split(';')
                parsed["mysql.innodb[total_memory_allocated]"] = dpower_tools.findNumber(splited[0])
                parsed["mysql.innodb[additional_pool_allocated]"] = dpower_tools.findNumber(splited[1])

            if "Buffer pool size" in line:
                parsed["mysql.innodb[buffer_pool_size]"] = dpower_tools.findNumber(line)

            if "Free buffers" in line:
                parsed["mysql.innodb[free_buffer]"] = dpower_tools.findNumber(line)

            if "Database pages" in line:
                parsed["mysql.innodb[database_pages]"] = dpower_tools.findNumber(line)

            if "Old database pages" in line:
                parsed["mysql.innodb[old_database_pages]"] = dpower_tools.findNumber(line)

            if "Modified db pages" in line:
                parsed['mysql.innodb[modified_db_pages]'] = dpower_tools.findNumber(line)

            if "Pending reads" in line:
                parsed['mysql.innodb[pending_reads]'] = dpower_tools.findNumber(line)

            if "Pending writes" in line:
                splited = line.split(',')
                parsed['mysql.innodb[pending_write_lru]'] = dpower_tools.findNumber(splited[0])
                parsed['mysql.innodb[pending_write_flush_list]'] = dpower_tools.findNumber(splited[1])
                parsed['mysql.innodb[pending_write_single_page]'] = dpower_tools.findNumber(splited[2])

            if "Pages made young" in line:
                splited = line.split(',')
                parsed['mysql.innodb[innodb_pages_made_young]'] = dpower_tools.findNumber(splited[0])
                parsed['mysql.innodb[innodb_pages_made_not_young]'] = dpower_tools.findNumber(splited[1])

            if "youngs/s" in line:
                splited = line.split(',')
                parsed['mysql.innodb[pages_young_seconds]'] = dpower_tools.findNumber(splited[0])
                parsed['mysql.innodb[pages_non_young_seconds]'] = dpower_tools.findNumber(splited[1])

            if "Pages read" in line:
                splited = line.split(',')
                parsed['mysql.innodb[pages_read]'] = dpower_tools.findNumber(splited[0])
                parsed['mysql.innodb[pages_created]'] = dpower_tools.findNumber(splited[1])
                parsed['mysql.innodb[pages_written]'] = dpower_tools.findNumber(splited[2])

            if "creates/s" in line:
                splited = line.split(',')
                parsed['mysql.innodb[pages_read_seconds]'] = dpower_tools.findNumber(splited[0])
                parsed['mysql.innodb[pages_created_seconds]'] = dpower_tools.findNumber(splited[1])
                parsed['mysql.innodb[pages_written_seconds]'] = dpower_tools.findNumber(splited[2])
        return parsed

    def __init__(self, login, password, host):
        self.host = host
        self.login = login
        self.password = password
        try:
            logger.debug('Connection into MySQL...')
            self.conn = mysql.connector.connect(user=login, password=password, host=host, database='mysql')
        except Exception as e:
            print(e)

    def mysql_show_variables(self):
        query = ("show variables;")
        cur = self.conn.cursor()
        cur.execute(query)
        all_variables = dict()

        for row in cur.fetchall():
            # row[1] = dpower_tools.convert_bool_to_int(row[1])
            all_variables["mysql.variables[{}]".format(row[0])] = dpower_tools.convert_bool_to_int(row[1])
        cur.close()
        all_variables["timestamp"] = timestamp
        return all_variables

    '''
        - Aborted_connects
        - Threads_connected
        - Threads_running

    '''

    def mysql_show_status(self):
        query = ("show status;")
        cur = self.conn.cursor()
        cur.execute(query)
        status = dict()
        for row in cur.fetchall():
            status["mysql.status[{}]".format(row[0])] = dpower_tools.convert_bool_to_int(row[1])
        cur.close()
        status["timestamp"] = timestamp
        return status

    def aggr_mysql_memory_usage(self):
        all_variables = self.mysql_show_variables()

        ''' Global Variables '''
        key_buffer_size = int(all_variables['mysql.variables[key_buffer_size]'])
        query_cache_size = int(all_variables['mysql.variables[query_cache_size]'])
        tmp_table_size = int(all_variables['mysql.variables[tmp_table_size]'])
        innodb_buffer_pool_size = int(all_variables['mysql.variables[innodb_buffer_pool_size]'])
        innodb_additional_mem_pool_size = int(all_variables['mysql.variables[innodb_additional_mem_pool_size]'])
        innodb_log_buffer_size = int(all_variables['mysql.variables[innodb_log_buffer_size]'])

        ''' Thread  Variables '''
        max_connection = int(all_variables['mysql.variables[max_connections]'])
        read_buffer_size = int(all_variables['mysql.variables[read_buffer_size]'])
        read_rnd_buffer_size = int(all_variables['mysql.variables[read_rnd_buffer_size]'])
        join_buffer_size = int(all_variables['mysql.variables[join_buffer_size]'])
        thread_stack = int(all_variables['mysql.variables[thread_stack]'])
        binlog_cache_size = int(all_variables['mysql.variables[binlog_cache_size]'])

        ''' Aggregation '''
        Global = key_buffer_size + query_cache_size + tmp_table_size + innodb_buffer_pool_size + innodb_additional_mem_pool_size + innodb_log_buffer_size
        PerThread = read_buffer_size + read_rnd_buffer_size + join_buffer_size + thread_stack + binlog_cache_size
        Total = Global + (max_connection * PerThread)

        total = {
            "mysql.memory[total]": Total,
            "timestamp": timestamp
        }
        return total

    def aggr_mysql_query_seconds(self):
        status = self.mysql_show_status()
        queries = int(status['mysql.status[Queries]'])
        uptime = int(status["mysql.status[Uptime]"])
        qps = {
            "mysql.status[aggr_qps]": "{:.2f}".format(queries / uptime),
            "timestamp": timestamp
        }
        return qps

    def mysql_show_engine_innodb_status(self):
        query = ("show engine innodb status")
        cur = self.conn.cursor()
        cur.execute(query)
        innodb_status = dict()
        pure_row = cur.fetchone()
        innodb_status = self.parse_innodb_engine(pure_row)
        innodb_status["timestamp"] = timestamp
        cur.close()
        return innodb_status

    def mysql_get_time(self):
        query = ("select now()")
        cur = self.conn.cursor()
        cur.execute(query)
        row = cur.fetchall()
        cur.close()
        mysql_date = row[0][0]
        ntp_date = dpower_tools.query_ntp(dict_setup["ntp_date_server"].split(','))
        variables = self.mysql_show_variables()
        mysql_timezone = variables["mysql.variables[time_zone]"]

        mysql_local_date = mysql_date
        if mysql_timezone.lower() != "utc":
            mysql_date = dpower_tools.convert_local_utc(mysql_date, mysql_timezone)

        status_time = {
            "mysql.time[now]": mysql_local_date.strftime("%Y-%m-%d %H:%M:%S"),
            "mysql.time[trapper_ntp_utc_now]": ntp_date,
            "mysql.time[trapper_ntp_server]": dict_setup["ntp_date_server"],
            "mysql.time[mysql_datetime_diff]": dpower_tools.datetime_diference(mysql_date, ntp_date),
            "mysql.time[default_timezone]": mysql_timezone,
            "timestamp": timestamp

        }
        return status_time

    def get_table_statistics(self):
        statisct_list = list()
        query = ("SELECT  ENGINE, TABLE_SCHEMA, "
                 "  TABLE_NAME, DATA_LENGTH as data_length , "
                 "  INDEX_LENGTH as index_length, round(DATA_FREE/ 1024/1024) as data_free "
                 "  from information_schema.tables  "
                 "  WHERE ENGINE not in ('MEMORY', 'PERFORMANCE_SCHEMA', 'NULL')  "
                 "  AND TABLE_SCHEMA NOT IN ('mysql', 'sys', 'information_scheam')"
                 "  AND data_length > 0;")
        cur = self.conn.cursor()
        cur.execute(query)
        data_length = 0
        index_length = 0

        for row in cur.fetchall():
            row_dic = {
                "Engine": row[0],
                "Table": "{}.{}".format(row[1], row[2]),
                "Data_lengh": float(row[3]),
                "Index_length": float(row[4]),
                "Data_free": float(row[5]),
                "timestamp": timestamp
            }
            fragmentation_rate = 0.0
            try:
                fragmentation_rate = (float(row[5]) / float((row[3] + row[4])))
            except:
                pass
            row_dic["Fragmentation_rate"] = fragmentation_rate
            statisct_list.append(row_dic)
            data_length += float(row[3])
            index_length += float(row[4])
        cur.close()
        dict_return = {
            "mysql.tablestats['aggr_data_length']": int(data_length),
            "mysql.tablestats['aggr_index_length']": int(index_length),
            "timestamp": timestamp,
            "mysql.tablestats['all_tables_log']": statisct_list
        }

        '''
        https://www.zabbix.com/documentation/3.4/manual/discovery/low_level_discovery?s[]=discovery&s[]=rule
        http://stackoverflow.com/questions/22529677/zabbix-sender-syntax-for-discovery-rules


        {"data": [{"{#TABLENAME}": "application.clientes", "{#TABLE_DATA_FREE}": "0.0", "{#TABLE_DATA_LENGH}": "0.0",
                   "{#TABLE_INDEX_LENGH}": "0.0", "{#FRAGMENTATION_INDEX_LENGH}"}]}

        '''
        return dict_return

    def get_slow_queries(self, min_time):
        query = ("SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, SUBSTRING("
                 "        TRIM( "
                 "            replace("
                 "                replace("
                 "                    replace(INFO, '\\n', ' '), '  ',' '"
                 "                ), '\\t', ' '"
                 "            )"
                 "        ), 1, 100"
                 "    ) AS INFO FROM INFORMATION_SCHEMA.PROCESSLIST"
                 " WHERE TIME > {}"
                 " AND COMMAND not in  ('Sleep')"
                 " AND  INFO is not NULL;").format(min_time)

        logger.debug("get_slow_queries QUERY: {}".format(query))
        cur = self.conn.cursor()
        cur.execute(query)
        list_rows = list()
        for row in cur.fetchall():
            row_dic = {
                "ID": row[0],
                "User": row[1],
                "Db": row[2],
                "Command": row[3],
                "Time": row[4],
                "State": row[5],
                "Info": row[6]
            }
            list_rows.append(row_dic)

        dic_return = {"mysql.slow['number_of_slowqueries']": len(list_rows),
                      "mysql.slow['queries_slow_log']": list_rows,
                      "mysql.slow['base_seconds']": min_time,
                      "timestamp": timestamp
                      }
        return dic_return


class MySQLStats(object):
    def __init__(self, login, password, host):
        self.mysql = MySQLInterface(login=login, password=password, host=host)

    def mysql_slow_queries(self):
        result = dpower_tools.trapper(self.mysql.get_slow_queries(30))
        return result

    def mysql_table_statistics(self):
        result = dpower_tools.trapper(self.mysql.get_table_statistics())
        return result

    def mysql_server_clock(self):
        result = dpower_tools.trapper(self.mysql.mysql_get_time())
        return result

    def mysql_innodb_status(self):
        array = self.mysql.mysql_show_engine_innodb_status()
        result = dpower_tools.trapper(array)
        return result

    def mysql_server_qps(self):
        result = dpower_tools.trapper(self.mysql.aggr_mysql_query_seconds())
        return result

    def mysql_server_memory(self):
        result = dpower_tools.trapper(self.mysql.aggr_mysql_memory_usage())
        return result

    def mysql_server_status(self):
        result = dpower_tools.trapper(self.mysql.mysql_show_status())
        return result

    def mysql_server_variables(self):
        result = dpower_tools.trapper(self.mysql.mysql_show_variables())
        return result

    def sentStats(self):
        logger.info("Sending cloudwatch rds events...")
        self.mysql_server_variables()
        self.mysql_server_status()
        self.mysql_innodb_status()
        self.mysql_server_memory()
        self.mysql_server_clock()
        self.mysql_server_qps()
        self.mysql_table_statistics()
        self.mysql_slow_queries()
