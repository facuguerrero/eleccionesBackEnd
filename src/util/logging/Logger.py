import logging

LOGGING_FILE_NAME = 'elections.log'
FORMATTING_STRING = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOGGING_LEVEL = logging.INFO


class Logger:
    __initialized = False

    def __init__(self, class_name):
        self._logger = Logger.build_logger(class_name)

    def info(self, message):
        self._logger.info(message)

    def error(self, message):
        self._logger.exception(message)

    def debug(self, message):
        self._logger.debug(message)

    def warning(self, message):
        self._logger.warning(message)

    @classmethod
    def build_logger(cls, class_name):
        if not cls.__initialized:
            # Handler for file writing
            file_handler = logging.FileHandler(LOGGING_FILE_NAME)
            # Handler for console output
            console_handler = logging.StreamHandler()
            # Configure
            logging.basicConfig(format=FORMATTING_STRING, level=LOGGING_LEVEL, handlers=[file_handler, console_handler])
            cls.__initialized = True
        return logging.getLogger(class_name)
