import logging
from datetime import datetime
import secret_settings
# import findspark


# LOGGING
path = './' + str(datetime.now())[:19].replace('-','_').replace(' ', '__').replace(':', '_') + '.log'

logger = logging.getLogger('spark_query')
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

fh = logging.FileHandler(path)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)


# INIT SPARK
# findspark.init()


# VARIABLES
USER = secret_settings.USER
PASS = secret_settings.PASS
DATABASE = secret_settings.DATABASE
PORT = secret_settings.PORT
HOST = secret_settings.HOST
URL = secret_settings.URL
DRIVER = secret_settings.DRIVER
AWS_KEY = secret_settings.AWS_KEY
AWS_PASS = secret_settings.AWS_PASS
S3_PATH = secret_settings.S3_PATH
