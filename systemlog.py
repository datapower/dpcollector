import logging
from config import *
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
