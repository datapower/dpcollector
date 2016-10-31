dict_setup = {
        "loglevel": "DEBUG",
        "logfile": "powercollector.log",
        "pidfile": "powercolletor.pid",
        "dbtype": "mysql",
        "dblogin": "mysqladmin",
        "dbpassword": "mysqladmin",
        "dbendpoint": "master.mysql.internal.io",
        "dbrole": "single", # single, master, slave

        "metric_sent_protocol": "zabbix",
        "metric_sent_server": "zabbix-server.internal.io",
        "metric_sent_hostname": "master.mysql",
        "metric_sent_cache_folder": "/var/spool/dpcollector/",
        "metric_sent_cicle_time_seconds": 30,

        "aws_cloud_watch": True,
        "aws_cloud_watch_period": 60,
        "aws_diff_between_start_end_minutes": 5,
        "aws_region": "us-east-1",
        "aws_rds_host_name": "zabbix",
        "aws_access_key_id": "",
        "aws_secret_access_key": "",


        "ntp_date_server": "us.pool.ntp.org"
        }

import logging
logger = logging.getLogger(__name__)
logger.setLevel(dict_setup["loglevel"])

try:
    dict_setup["logfile"]
    handler = logging.FileHandler(dict_setup["logfile"])
except:
    handler = logging.StreamHandler()

handler.setLevel(dict_setup["loglevel"])
formatter = logging.Formatter('%(levelname)s:%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
