import datetime
import logging

logger = logging.getLogger(__name__)

def datetimeField(current_datetime:datetime.datetime):
    logger.info('datetimeField()')
    logger.info(str(current_datetime))
    if current_datetime.hour < 16:
        logger.info('Before 4:00pm')
        res = (current_datetime - datetime.timedelta(1))\
            .strftime('%Y%m%d')
    else:
        logger.info('After 4:00pm')
        res = (current_datetime).strftime('%Y%m%d')
    logger.info(res)
    return res