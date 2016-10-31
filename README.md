dpcollector
===========

Python collector for OS and DataBases like PostgreSQL, MySQL, DB2, Mongo and Web Services

* Pre-reque
- sudo pip3 install py-zabbix
- sudo pip3 install ntplib
- sudo pip3 install boto3
- sudo pip3 install mysql-connector
- sudo pip3 install pytz
- sudo pip3 install python-daemon

Apply this privileges on the monitored MySQL:
GRANT SELECT, CREATE USER, REPLICATION CLIENT, SHOW DATABASES, SUPER, PROCESS ON *.* TO  'powercollector'@'%' IDENTIFIED BY 'C0LL3cT0R2';
