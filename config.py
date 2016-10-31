'''

Apply this privileges on the monitored MySQL:
GRANT SELECT, CREATE USER, REPLICATION CLIENT, SHOW DATABASES, SUPER, PROCESS ON *.* TO  'powercollector'@'%' IDENTIFIED BY 'C0LL3cT0R2';

'''
dict_setup = {
        "loglevel": "DEBUG",
        "logfile": "powercollector.log",
        "pidfile": "powercolletor.pid",
        "dbtype": "mysql",
        "dblogin": "powercollector",
        "dbpassword": "C0LL3cT0R2",
        "dbendpoint": "master.mysql.internal.io",
        "dbrole": "single", # single, master, slave

        "metric_sent_protocol": "zabbix",
        "metric_sent_server": "zabbix-server.internal.io",
        "metric_sent_hostname": "master.mysql",
        "metric_sent_cache_folder": "/var/spool/dpcollector/",
        "metric_sent_cicle_time_seconds": 60,

        "aws_cloud_watch": True,
        "aws_cloud_watch_period": 60,
        "aws_diff_between_start_end_minutes": 5,
        "aws_region": "us-east-1",
        "aws_rds_host_name": "zabbix",
        "aws_access_key_id": "",
        "aws_secret_access_key": "",

        "ntp_date_server": "us.pool.ntp.org"
        }
