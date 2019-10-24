import pandas as pd
import numpy as np

from collections import namedtuple
from ...utils.date_utils import dt2uts_ms
from ...utils.toml_utils import get_config
from ...utils.file_utils import init_dir, write_parquet
from ...utils.log_utils import get_logger
from nbc_analysis.utils.debug_utils import retval

log = get_logger(__name__)

DayRecord = namedtuple('DayRecord', ['day_key', 'ordinal',
                                     'day', 'month', 'year', 'dayofweek', 'day_name', 'week_day',
                                     'month_name', 'days_in_month',
                                     'quarter', 'dayofyear', 'week', 'weekday', 'weekofyear', 'week_id', 'is_leap_year',
                                     'is_week_start', 'is_week_end', 'is_month_start', 'is_month_end',
                                     'is_quarter_start', 'is_quarter_end', 'is_year_start', 'is_year_end',
                                     'start_time', 'end_time', 'start_uts_ms', 'end_uts_ms',
                                     ])


def get_day_key(day):
    return int(str(day).replace('-', ''))


DAY_OF_WEEK_FIELDS = ['dayofweek', 'day_name', 'week_day']


def get_period_info(day):
    day_key = get_day_key(day)
    end_time = (day.start_time + pd.DateOffset(1))

    rec = DayRecord(
        day_key=day_key,
        ordinal=day.ordinal,
        dayofweek=day.dayofweek,
        week_day='week day' if day.dayofweek not in (5, 6) else 'week end',
        dayofyear=day.dayofyear,
        day_name=day.start_time.day_name(),
        month_name=day.start_time.month_name(),
        days_in_month=day.days_in_month,
        is_leap_year=int(day.is_leap_year),
        day=day.day,
        month=day.month,
        year=day.year,
        week=day.week,
        weekday=day.weekday,
        weekofyear=day.weekofyear,
        week_id=f"{day.year}W{day.weekofyear:02d}",

        quarter=day.start_time.quarter,
        is_week_start=(1 if day.weekday == 0 else 0),
        is_week_end=(1 if day.weekday == 6 else 0),
        is_month_start=int(day.start_time.is_month_start),
        is_month_end=int(day.start_time.is_month_end),
        is_quarter_start=int(day.start_time.is_quarter_start),
        is_quarter_end=int(day.start_time.is_quarter_end),
        is_year_start=int(day.start_time.is_year_start),
        is_year_end=int(day.start_time.is_year_end),
        start_time=day.start_time.isoformat(),
        end_time=end_time.normalize().isoformat(),
        start_uts_ms=dt2uts_ms(day.start_time),
        end_uts_ms=dt2uts_ms(end_time),
    )
    return rec


def cr_hour_in_week_dim(day_dim):
    df = day_dim[DAY_OF_WEEK_FIELDS].drop_duplicates().sort_values('dayofweek')

    reader = np.arange(24)
    reader = (df.assign(hour=x) for x in reader)
    df = pd.concat(reader).sort_values(['dayofweek', 'hour'])
    df['hour_in_week_key'] = (df.dayofweek * 24) + df.hour
    time_period_idx = (df.hour // 6)
    df['time_period'] = time_period_idx.map(lambda x: ['early morning', 'morning', 'afternoon', 'evening'][x])

    df = df.reindex(columns=['hour_in_week_key',
                             'hour',
                             'time_period',
                             'dayofweek',
                             'day_name',
                             'week_day',
                             ])
    df = df.rename(columns={'dayofweek': 'dayofweek_hiw', 'day_name': 'day_name_hiw', 'week_day': 'week_day_hiw'})
    return df


def main(config):
    config = get_config(config=config)
    cfg = config['calendar']

    log.info(f"start cr_cal_dims,cfg={cfg}")
    start_day_key = cfg['start_day_key']
    end_day_key = cfg['end_day_key']
    calendar_d = init_dir(cfg['calendar_d'], exist_ok=True, parents=True, rmtree=True)

    log.info(f"event generate day calendar")
    dates = pd.period_range(start=start_day_key, end=end_day_key, )[:-1]
    reader = map(get_period_info, dates)  # creates DayRecord tuples
    day_dim = pd.DataFrame.from_records(reader, columns=DayRecord._fields)
    write_parquet('dim_day', df=day_dim, outdir=calendar_d)

    log.info(f"event generate local day calendar")
    # create dim for local timezone, renaming fields so easier to query
    # TODO: Add support for views instead of copying dataset
    df = day_dim.copy()
    df.columns = map(lambda field: f"{field}_loc", df.columns)
    write_parquet('dim_day_loc', df=df, outdir=calendar_d)

    log.info(f"event generate hour in week")
    df = cr_hour_in_week_dim(day_dim)
    write_parquet('dim_hour_in_week', df=df, outdir=calendar_d)
    log.info(f"end cr_cal_dims")
    return day_dim
