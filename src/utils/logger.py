import datetime as dt
import logging

from src.utils.constants import LOGGING_FORMAT, LOGGING_PATH


def scrapper_logger(name):
    """
    :param name: str, name of the scrapper we are working with
    :return: logging.Logger, the logger associated with this fund, with specific path and logs names
    """
    logging_time = dt.datetime.now().strftime('%Y-%m-%d')
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(LOGGING_FORMAT)
    file_handler = logging.FileHandler(logging_time + '_' + name + '.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def logger_init(l):
    l.info('Initializing ' + l.name)
