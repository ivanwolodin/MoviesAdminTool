import logging

from constants import (
    ETL_LOG_FILENAME,
    LOGGER_NAME,
    LOGGER_ENCODING,
    LOGGER_FORMAT,
)

logging.basicConfig(
    filename=ETL_LOG_FILENAME,
    encoding=LOGGER_ENCODING,
    level=logging.DEBUG,
    format=LOGGER_FORMAT,
)

logger = logging.getLogger(LOGGER_NAME)
