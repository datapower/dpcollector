import logging
import time
from mysql_interface import MySQLStats
from aws_interface import AWSStats
from config import *

logger = logging.getLogger(__name__)

try:
    dict_setup["logfile"]
    handler = logging.FileHandler(dict_setup["logfile"])
except:
    handler = logging.StreamHandler()

handler.setLevel(dict_setup["loglevel"])
formatter = logging.Formatter('%(levelname)s:%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def aws():
    logger.debug("Running AWSStats...")
    aws_interface = AWSStats()
    aws_interface.sentStats()


def mysql():
    logger.debug("Running MySQLStats...")
    my_interface = MySQLStats(login=dict_setup["dblogin"], password=dict_setup["dbpassword"],
                              host=dict_setup["dbendpoint"])
    my_interface.sentStats()


def collector():
    logger.info("Starting Power Collector...")
    #while True:
    aws()
    #mysql()
    time.sleep(dict_setup["metric_sent_cicle_time_seconds"])

if __name__ == "__main__":
    logger.debug("Starting...")
    collector()
