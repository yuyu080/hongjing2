from util_Log.logger  import logging
from config.conf import config
from bbdSDK.quant import m
m()
logger = logging.getLogger(__name__)
logger.debug('This is info message')
logger.info('This is info message')
logger.warning('This is warning message')
logger.exception("thisi is a exception")

logger.exception("thisi is a exception")
logger.error('Failed to open file', exc_info=True)
print(config.getConf("spark","a"))



