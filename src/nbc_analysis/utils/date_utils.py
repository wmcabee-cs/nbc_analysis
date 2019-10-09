import arrow
import re
import pandas as pd
import numpy as np


# TODO: add round trip unit test for calendar utilities


def parse_day(day):
    m = re.match(r"(?P<year>\d{4})(?P<month>\d{2})(?P<day_num>\d{2})", day)
    if not m:
        raise ValueError(f"Invalid date format '{day}'")

    return m.groupdict()


def get_now_zulu():
    return arrow.utcnow().isoformat('T').replace('+00:00', 'Z')


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
