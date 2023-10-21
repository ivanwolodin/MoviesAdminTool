import logging

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(
    filename='etl.log',
    encoding='utf-8',
    level=logging.DEBUG,
    format=FORMAT,
)
logger = logging.getLogger('postgres_uploader')