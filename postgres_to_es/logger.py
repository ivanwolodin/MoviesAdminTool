import logging

from constants import (
    ETL_LOG_FILENAME,
    LOGGER_NAME,
    LOGGER_ENCODING,
)

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(
    filename=ETL_LOG_FILENAME,
    encoding=LOGGER_ENCODING,
    level=logging.DEBUG,
    format=FORMAT,
)

logger = logging.getLogger(LOGGER_NAME)
