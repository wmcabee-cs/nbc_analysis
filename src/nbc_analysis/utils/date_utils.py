import arrow
import re
import pandas as pd
import numpy as np

from nbc_analysis.utils.log_utils import get_logger

log = get_logger(__name__)


# TODO: add round trip unit test for calendar utilities


def parse_day(day):
    m = re.match(r"(?P<year>\d{4})(?P<month>\d{2})(?P<day_num>\d{2})", day)
    if not m:
        raise ValueError(f"Invalid date format '{day}'")

    return m.groupdict()


def get_now_zulu():
    return arrow.utcnow().isoformat('T').replace('+00:00', 'Z')


def get_today():
    return arrow.get().format("YYYYMMDD")


def check_day_key(astr: str):
    try:
        ts = pd.to_datetime(astr, format="%Y%m%d")
        return True

    except ValueError as e:
        log.exception(f"Problem parsing day value '{astr}'. Expecting format 'YYYYMMDD")
        return False


#################################################################
# Conversion between nbc unixtime_ms format to pandas ds and back
#################################################################


# scalar conversion
def dt2uts_ms(dt):
    return int(dt.timestamp() * 1000)


def uts_ms2dt(uts_ms):
    return pd.to_datetime(uts_ms / 1000, unit='s', origin='unix')


##########
# data series conversion
def ds_dt2uts_ms(ds):
    return (ds.astype(np.int) / 10 ** 6).astype(np.int)


def ds_uts_ms2dt(ds):
    return pd.to_datetime(ds / 1000, unit='s', origin='unix')


END_OF_TIME_DAY_KEY = 21111111


def get_end_of_time_ms():
    dt = pd.to_datetime(str(END_OF_TIME_DAY_KEY))
    return dt2uts_ms(dt)


END_OF_TIME = get_end_of_time_ms()
