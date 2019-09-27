from typing import Dict, Optional
import re
import time
from dataclasses import dataclass
import arrow
from toolz import take, partial, first
import pandas as pd

from ..utils.file_utils import init_dir
from ..utils.aws_utils import get_bucket
from ..utils.config_utils import get_config
from ..utils.debug_utils import retval

'''
def write_events():
    extract_f = f"{extract}_{day}.csv"
    outfile = workdir / extract_f
    df['day'] = day
    df['extract'] = extract
    df['extract_f'] = extract_f
    df.to_csv(outfile, index=False)
    df = df.sort_values(['size'])
    print(f">> end extract,extract={extract},outfile={outfile},records={len(df)}")
'''


def parse_day(day):
    m = re.match(r"(?P<year>\d{4})(?P<month>\d{2})(?P<day_num>\d{2})", day)
    if not m:
        raise ValueError(f"Invalid date format {day}")

    return m.groupdict()


def take_if_limit(reader, limit):
    return take(limit, reader) if limit is not None else reader


def get_now_zulu():
    return arrow.utcnow().isoformat('T').replace('+00:00', 'Z')


# TODO: Seperate code specific to this project and reusable components
@dataclass
class CortexListerS32DF(object):
    config: Dict

    def read(self, days):
        # Initialize from config
        config = self.config
        bucket_key = config['VIDEO_END_BUCKET']
        limit_events_per_batch = config['LIMIT_EVENTS_PER_BATCH']

        # initialize IO
        bucket = get_bucket(bucket_key)

        # constants
        extract_spec = 'year={year}/month={month}/event=video_end/NBCUniversal_{day}'

        # detail function
        def list_objects_for_day(day):
            asof_dt = get_now_zulu()
            day_info = parse_day(day)
            prefix = extract_spec.format(day=day, **day_info)
            print(f">> start list_objects_for_day,day={day},prefix={prefix},asof_dt={asof_dt}")

            start = time.time()
            reader = bucket.objects.filter(Prefix=prefix).all()
            reader = take_if_limit(reader, limit_events_per_batch)
            reader = ((obj.key, obj.size) for obj in reader)
            df = pd.DataFrame.from_records(reader, columns=['name', 'size'])
            duration = round(time.time() - start, 5)

            # add info to dataframe
            df['batch_id'] = day
            df['asof_dt'] = asof_dt
            print(f"end list_objects_for_day,day={day},asof={asof_dt},cnt={len(df)},duration={duration}")
            return (day, asof_dt), df

        # iteration
        return map(list_objects_for_day, days)


class CortexWriterDF2LocalParquet(object):
    config: Dict

    def write(self, reader):
        # read configuration
        config = self.config
        extract_d = config['EXTRACTS_D']
        extract_d = init_dir(extract_d, parents=True, exist_ok=True, rmtree=False)


def save_params(local_dict: Dict, exclude):
    return {k.upper(): v
            for k, v in local_dict.items()
            if k not in exclude}


def main(config_f=None, batch_limit=None, limit_events_per_batch=None, limit_batch_cnt=None):
    overrides = save_params(locals(), exclude={'config_f'})

    ###################
    # Resolve configuration
    ###################
    config = get_config(overrides=overrides, config_f=config_f)

    ####################
    # TODO: !! Remove DAYS from config. pass list of as input
    #    In config temporarily while developing
    ####################
    days = config['DAYS']

    ####################
    # Initialize IO
    ####################
    # remove days so doesn't clutter log
    log_config = {k: v for k, v in config.items() if k != 'DAYS'}
    print(f">> start extract run, config={log_config}")

    source = CortexListerS32DF(config)

    # Initialize IO
    reader = days
    reader = take_if_limit(reader, limit=batch_limit)
    reader = source.read(reader)
    retval(first(reader))
    for x in reader: pass
    print(">> end extract run")
