import logging
import shutil
import sys
from os import mkdir
from os.path import join, exists
from setting import LOG_LEVEL, ENABLE_LOG_FILE, ENABLE_LOG_RUNTIME_FILE, env, LOG_DIR, ENABLE_LOG_ERROR_FILE, \
    ENABLE_LOG, ENABLE_LOG_CONSOLE

loggers = {}

LOG_FORMAT = '%(levelname)s - %(asctime)s - pid: %(process)d -tid: %(thread)d - %(name)s - %(module)s - %(message)s'


def get_logger(name=None):
    """
    get logger by name
    :param name: name of logger
    :return: logger
    """
    global loggers

    if not name:
        name = __name__

    if loggers.get(name):
        return loggers.get(name)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    # 输出到控制台
    if ENABLE_LOG and ENABLE_LOG_CONSOLE:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level=LOG_LEVEL)
        formatter = logging.Formatter(LOG_FORMAT)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if ENABLE_LOG and ENABLE_LOG_FILE:
        if ENABLE_LOG_RUNTIME_FILE:
            if not exists(LOG_DIR):
                mkdir(LOG_DIR)
            log_file = join(LOG_DIR, 'runtime.log')
            if not exists(log_file):
                open(log_file, 'x').close()
            file_handler_runtime = logging.FileHandler(log_file)
            formatter = logging.Formatter(LOG_FORMAT)
            file_handler_runtime.setFormatter(formatter)
            logger.addHandler(file_handler_runtime)
        if ENABLE_LOG_ERROR_FILE:
            if not exists(LOG_DIR):
                mkdir(LOG_DIR)
            log_file = join(LOG_DIR, 'error.log')
            if not exists(log_file):
                open(log_file, 'w').close()
            file_handler_error = logging.FileHandler(log_file)
            formatter = logging.Formatter(LOG_FORMAT)
            file_handler_error.setFormatter(formatter)
            logger.addHandler(file_handler_error)
    else:
        shutil.rmtree(LOG_DIR, ignore_errors=True)
    # 保存到全局 loggers
    loggers[name] = logger
    return logger
