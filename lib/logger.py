class Log4j(object):
    def __init__(self, spark):
        Log4j = spark._jvm.org.apache.log4j
        self.logger = Log4j.LogManager.getLogger('retail_analysis')

    def error(self, message):
        self.logger.error(message)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

