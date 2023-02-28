import logging
import sys

from src.kz.krisha import send_email_krisha
from src.utils.logger import scrapper_logger

logging.getLogger('WDM').setLevel(logging.NOTSET)

logger = scrapper_logger('Krisha')

try:
    logger.info(logger.name + ' - Starting Krisha Research Script')
    city = sys.argv[1]
    jk = sys.argv[2]
    room_number = int(sys.argv[3])
    environment = sys.argv[4]
    logger.info(
        f'{logger.name} in {environment}- Arguments: \n    city: {city}\n    jk: {jk}\n    room_number: {room_number}')
    send_email_krisha(city=city, jk_name=jk, number_of_rooms=room_number, environment=environment)
    logger.info(logger.name + ' -  Finished')
except Exception as e:
    logger.error(logger.name + ' -  Error {}'.format(e), exc_info=True)
    logger.info(logger.name + ' -  Aborted')
