from data_transfer import DataTransfer
from logger import logger


def load_from_sqlite():
    """Основной метод загрузки данных из SQLite в Postgres"""
    data_transfer_obj = DataTransfer()
    data_transfer_obj.transfer_data()


if __name__ == '__main__':
    print('Starting transposing')
    logger.info('Starting data transferring')
    load_from_sqlite()
    logger.info('Stop data transferring')
    print('Successful finish!')
