#-*- coding: UTF-8 -*-  

import json
import logging.config

from config.conf import config


path = config.getLogConfig()
with open(path, 'rt') as f:
    logconfig = json.load(f)
logging.config.dictConfig(logconfig)

if __name__ == '__main__':
    pass