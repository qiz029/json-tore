# -- coding: utf-8 --

import logging

class json_log(object):

    def __init__(self, className='JSONTORE', log_level=logging.INFO):
        self.logger = logging.getLogger(className)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        self.logger.setLevel(log_level)

    def info(self, msg):
        self.logger.info(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

    def fatal(self, msg):
        self.logger.fatal(msg)
