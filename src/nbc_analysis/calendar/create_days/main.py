import pandas as pd
from collections import namedtuple
from nbc_analysis.utils.date_utils import dt2uts_ms
from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.utils.file_utils import init_dir
from pathlib import Path
from nbc_analysis.utils.debug_utils import retval
from toolz import first

START_DAY = '20170101'
END_DAY = '20200101'

DayRecord = namedtuple('DayRecord', ['day_key', 'ordinal',
                                     'day', 'month', 'year', 'dayofweek', 'day_name', 'month_name', 'days_in_month',
                                     'quarter', 'dayofyear', 'week', 'weekday', 'weekofyear', 'week_id', 'is_leap_year',
                                     'is_week_start', 'is_week_end', 'is_month_start', 'is_month_end',
                                     'is_quarter_start', 'is_quarter_end', 'is_year_start', 'is_year_end',
                                     'start_time', 'end_time', 'start_uts_ms', 'end_uts_ms',
                                     ])


def get_day_key(day):
    return int(str(day).replace('-', ''))


def get_period_info(day):
    day_key = get_day_key(day)
    end_time = (day.start_time + pd.DateOffset(1))

    rec = DayRecord(
        day_key=day_key,
        ordinal=day.ordinal,
        dayofweek=day.dayofweek,
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


def main(config):
    config = get_config(config=config)
    start_day_key = config['CALENDAR_START_DAY_KEY']
    end_day_key = config['CALENDAR_END_DAY_KEY']
    calendar_d = Path(config['CALENDAR_D'])
    print(f">> creating days calendar,start_day_key={start_day_key},end_day_key={end_day_key}")
    init_dir(calendar_d, exist_ok=True, parents=True)

    dates = pd.period_range(start=start_day_key, end=end_day_key, )[:-1]
    df = pd.DataFrame.from_records(map(get_period_info, dates), columns=DayRecord._fields)

    # write dataset
    outfile = calendar_d / 'cal_days.parquet'
    df.to_parquet(outfile, index=False)
    print(f">> wrote file,outfile={outfile},records={len(df)}")
