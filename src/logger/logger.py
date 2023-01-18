"""
This file is utility function called get loggingSession to return a valid logging session
"""
import logging


def get_logging_session():
    # create logger
    logger_obj = logging.getLogger()
    logger_obj.setLevel(logging.INFO)

    # create console handler and set level to debug
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('%(asctime)s %(levelname)s: ' + 'Line - ' + '%(lineno)d' + ':' + '%(message)s',
                                  "%Y%m%d%H%M%S")

    # add formatter to handler
    console_handler.setFormatter(formatter)

    # add handlers to loggers
    logger_obj.addHandler(console_handler)

    return logger_obj


logger = get_logging_session()
