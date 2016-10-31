from munch import munchify  # https://github.com/Infinidat/munch
from config import dict_setup
import logging
import re
import re
from pyzabbix import ZabbixMetric, ZabbixSender
import inspect
import ntplib
from datetime import datetime
# import datetime
import ntplib
import pytz

import time
from functools import wraps


def retry(ExceptionToCheck, tries=4, delay=1, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """

    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    msg = "{}, Retrying in {} seconds...".format(str(e), mdelay)
                    if logging:
                        logging.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


def getconfig(config_name=None):
    #    if config_name is not None:
    #        return munchify(dict_setup[config_name])
    #    else:
    return munchify(dict_setup)


def findNumber(text):
    return (re.findall(r'\d+', text))


def parse_innodb_engine(text):
    teste = text[2]

    parsed = dict()
    for line in teste.split("\n"):
        print(line)

        if "srv_master_thread log flush and writes" in line.split(':'):
            parsed['innodb_srv_master_thread_log'] = line.split(':')[1]

        if "OS WAIT ARRAY INFO: reservation count" in line:
            parsed['innodb_os_wait_reservation_count'] = findNumber(line)

        if "OS WAIT ARRAY INFO: signal count" in line:
            parsed['innodb_os_wait_signal_count'] = findNumber(line)

        if "Mutex spin waits" in line:
            splited = line.split(',')
            parsed['innodb_mutex_spin_waits'] = findNumber(splited[0])
            parsed['innodb_mutex_spin_rounds'] = findNumber(splited[1])
            parsed['innodb_mutex_spin_os_waits'] = findNumber(splited[2])

        if "Total memory allocated" in line:
            splited = line.split(';')
            parsed["innodb_total_memory_allocated"] = findNumber(splited[0])
            parsed["innodb_additional_pool_allocated"] = findNumber(splited[1])

        if "Buffer pool size" in line:
            parsed["innodb_buffer_pool_size"] = findNumber(line)

        if "Free buffers" in line:
            parsed["innodb_free_buffer"] = findNumber(line)

        if "Database pages" in line:
            parsed["innodb_database_pages"] = findNumber(line)

        if "Old database pages" in line:
            parsed["innodb_old_database_pages"] = findNumber(line)

        if "Modified db pages" in line:
            parsed['innodb_modified_db_pages'] = findNumber(line)

        if "Pending reads" in line:
            parsed['innodb_pending_reads'] = findNumber(line)

        if "Pending writes" in line:
            splited = line.split(',')
            parsed['innodb_pending_write_lru'] = findNumber(splited(',')[0])
            parsed['innodb_pending_write_flush_list'] = findNumber(splited(',')[1])
            parsed['innodb_pending_write_single_page'] = findNumber(splited(',')[2])

        if "Pages made young" in line:
            splited = line.split(',')
            parsed['innodb_pages_made_young'] = findNumber(splited.split(',')[0])
            parsed['innodb_pages_made_not_young'] = findNumber(splited.split(',')[1])

        if "youngs/s" in line:
            splited = line.split(',')
            parsed['innodb_pages_young_seconds'] = findNumber(splited.split(',')[0])
            parsed['innodb_pages_non_young_seconds'] = findNumber(splited.split(',')[1])

        if "Pages read" in line:
            splited = line.split(',')
            parsed['innodb_pages_read'] = findNumber(splited.split(',')[0])
            parsed['innodb_pages_created'] = findNumber(splited.split(',')[1])
            parsed['innodb_pages_written'] = findNumber(splited.split(',')[2])

        if "creates/s" in line:
            splited = line.split(',')
            parsed['innodb_pages_read_seconds'] = findNumber(splited.split(',')[0])
            parsed['innodb_pages_created_seconds'] = findNumber(splited.split(',')[1])
            parsed['innodb_pages_written_seconds'] = findNumber(splited.split(',')[2])

    return parsed


def convert_bool_to_int(value):
    try:
        if isinstance(value, bool):
            if value:
                return 1
            else:
                return 0
        if isinstance(value, str):
            if value.lower() == "yes" or value.lower() == "true" or value == "1" or value.lower() == "on":
                return 1
            elif value.lower() == "no" or value.lower() == "false" or value == "1" or value.lower() == "off":
                return 0
            else:
                return value
        else:
            return value
    except:
        return value


def trapper(items_raw):
    if dict_setup["metric_sent_protocol"].lower() == "zabbix":
        hostname = dict_setup["metric_sent_hostname"].lower()
        zabbix_server = dict_setup["metric_sent_server"].lower()
        try:
            timestamp = items_raw['timestamp']
            metrics = []
            zbx = ZabbixSender(zabbix_server)
            for metric in items_raw:
                if metric != "timestamp":
                    m = ZabbixMetric(host=hostname, key=metric, value=items_raw[metric], clock=timestamp)
                    metrics.append(m)
            returapi = zbx.send(metrics)
            logging.info("{}: {}".format(inspect.stack()[1][3], returapi))
            return True
        except Exception as e:
            logging.error("Trappto zabbix error: {} -  {}".format(inspect.stack()[1][3], e))
            return False
    else:
        return False


@retry(Exception, tries=4)
def query_ntp(ntp_server, version=3):
    c = ntplib.NTPClient()
    response = c.request(ntp_server, version=version)
    utctime = datetime.utcfromtimestamp(response.tx_time).strftime("%Y-%m-%d %H:%M:%S")
    return utctime


def convert__to_utc_timezone(timezone):
    utc_time = datetime.utcnow()
    tz = pytz.timezone('America/St_Johns')

    utc_time = utc_time.replace(tzinfo=pytz.UTC)  # replace method
    st_john_time = utc_time.astimezone(tz)  # astimezone method
    print(st_john_time)


'''
Return the diference between two datatime but older against newer.
'''


def datetime_diference(datatime_a, datatime_b):
    if isinstance(datatime_a, str):
        datetime_a = datetime.strptime(datatime_a, "%Y-%m-%d %H:%M:%S")
    if isinstance(datatime_b, str):
        datatime_b = datetime.strptime(datatime_b, "%Y-%m-%d %H:%M:%S")

    if (datatime_a > datatime_b):
        return (datatime_a - datatime_b).seconds
    else:
        return (datatime_b - datatime_a).seconds


'''
    format(e.g., 2016-10-03T23:00:00Z).
'''


def datetime_iso8601(dt):
    iso8601 = dt.strftime("%Y-%m-%dT%H:%M:00Z")
    return iso8601
