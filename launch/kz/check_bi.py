import logging
import sys

from src.kz.bi_group import send_email_bi
from src.utils.logger import scrapper_logger

logging.getLogger('WDM').setLevel(logging.NOTSET)

logger = scrapper_logger('BI_Group')

try:
    logger.info(logger.name + ' - Starting Krisha Research Script')
    city = sys.argv[1]
    jk = sys.argv[2]
    room_number = int(sys.argv[3])
    logger.info(logger.name + ' - Arguments: \n    city: ' + city + '\n    jk: ' + jk +
                '\n    room_number: ' + str(room_number))
    send_email_bi(city=city, jk_name=jk, number_of_rooms=room_number)
    logger.info(logger.name + ' -  Finished')
except Exception as e:
    logger.error(logger.name + ' -  Error {}'.format(e), exc_info=True)
    logger.info(logger.name + ' -  Aborted')
