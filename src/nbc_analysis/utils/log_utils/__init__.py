import logging
from pathlib import Path
from ..file_utils import init_dir
import os


def fmt_cfg(cfg):
    return ','.join(f"{k}={v}" for k, v in cfg.items())


LOGGING_INITIALIZED = False


def setup_logging():
    global LOGGING_INITIALIZED
    if LOGGING_INITIALIZED:
        return

    NBC_DATA_TOP = os.environ['NBC_DATA_TOP']
    if NBC_DATA_TOP is None:
        raise Exception("Must set environment variable NBC_DATA_TOP")

    log_d = Path(NBC_DATA_TOP) / 'logs'
    init_dir(log_d, exist_ok=True, parents=True)
    log_f = log_d / 'nbc_analysis.log'

    # Gets or creates a logger
    logger = logging.getLogger('nbc_analysis')

    # set log level
    logger.setLevel(logging.DEBUG)

    # define file handler and set formatter
    fh = logging.FileHandler(str(log_f))
    fh.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s|%(levelname)s|%(name)s|%(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add file handler to logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    LOGGING_INITIALIZED = True


def get_logger(name):
    return logging.getLogger(name)


setup_logging()
