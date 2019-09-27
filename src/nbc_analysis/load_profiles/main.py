from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.config_utils import get_config
from pathlib import Path
from toolz import concatv, first, cons, partial, concat, take
from itertools import starmap
import pandas as pd
import yaml
from toolz import pluck
import re
import arrow
import numpy as np
from timeit import default_timer as timer
import pprint

from cortex import Cortex

from cortex_common.types import EntityEvent
from cortex_profiles import ProfileBuilder, ProfileClient

SCRIPT_D = Path(__file__).parent.absolute()


def read_files(indir):
    reader = concatv(indir.glob('*.csv'), indir.glob('*.csv.gz'))
    reader = map(lambda x: (x, pd.read_csv(x)), reader)
    reader = list(reader)
    print(f">> listing input files, dir={indir},file_cnt={len(reader)}")
    return reader


def load_schema(cortex_schema_version):
    # TODO: Move to data resource so can use from python package
    json_f = SCRIPT_D / 'profile_schema.json'
    with json_f.open() as fh:
        schema = yaml.safe_load(fh)
    if cortex_schema_version != schema['name']:
        raise Exception(
            f"Schema name doesn't match, cortex_schema_version={cortex_schema_version},name in schema{schema['name']}")
    return schema


def get_interfaces(schema_version):
    cortex = Cortex.client()
    builder = ProfileBuilder(cortex)
    profile_schema = builder.profiles(schema_version)
    return profile_schema


def get_column_list(schema_version):
    schema = load_schema(schema_version)
    return list(pluck('name', schema['attributes']))


regex = re.compile("(?P<year>\d\d\d\d)(?P<month>\d\d)(?P<day>\d\d)")


def fix_day(day):
    m = regex.match(day)
    return '/'.join([m.group('month'), m.group('day'), m.group('year')])


def make_sample(df, sample_size):
    if sample_size is None:
        return df
    df = df[df.mpid > 0]
    return df[slice(None, sample_size)].copy()


def clean_data(file, df, column_list, sample_size=None):
    """ Reformat dates and fix null values """

    df = make_sample(df, sample_size)

    # Add event time and adjust day format
    # TODO: Move to concat script
    df['day'] = df['day'].astype(str).map(fix_day)
    df['event_time'] = df['day'].map(lambda dy: arrow.get(dy).timestamp * 1000)
    df = df[list(cons('event_time', column_list))]

    # TOOD: Handle infinity values
    df = df.mask(df.isin([np.inf, -np.inf]))
    # df.replace([np.inf, -np.inf], np.nan)

    # TODO: Handle null values.  Setting to 0 until get way of handling nulls from Omar
    clean_columns = ['ads_per_video_cnt_avg', 'ad_vs_video_time_avg']
    df[clean_columns] = df[clean_columns].fillna(0.)
    return file, df


def build_file_events(file, df, schema_version):
    record_cnt = len(df)
    file_records = ((idx, [EntityEvent(event=field,
                                       entityId=str(row.mpid),
                                       entityType=schema_version,
                                       eventTime=row.event_time,
                                       properties={"value": value},
                                       # meta={},
                                       )
                           for field, value in zip(row._fields, row) if field != 'event_time'])
                    for idx, row in enumerate(df.itertuples(index=False), start=1))
    return file, record_cnt, file_records


def write_record(file, record_cnt, idx, record, profile_schema, skip_writes, stop_on_fail):
    ret = {''
           'file': file,
           'record_cnt': record_cnt,
           'idx': idx,
           'start_ts': arrow.utcnow().isoformat(),
           'end_ts': None,
           'status': 'error',
           'exception': 'unknown',
           'exception_type': 'unknown'}
    try:
        start = timer()
        if not skip_writes:
            profiles = profile_schema.with_events(record)
            profiles.build()
        ret['status'] = 'OK'
        del ret['exception']
        del ret['exception_type']
        return ret
    except Exception as e:
        ret['exception'] = str(e)
        ret['exception_type'] = str(type(e))
        print(f">> error,{ret}")
        if stop_on_fail:
            raise
        return ret
    finally:
        end = timer()
        ret['duration'] = end - start
        ret['end_ts'] = arrow.utcnow().isoformat()
        print( f'>>write record,{ret["file"].name},{ret["idx"]},{ret["record_cnt"]},{ret["status"]},{round(ret["duration"], 5)}')


def main(sample_size=25, skip_writes=False, stop_on_fail=True):
    config = get_config()

    concat_d = Path(config['CONCAT_D'])
    schema_version = config['CORTEX_SCHEMA_VERSION']
    profile_schema = get_interfaces(schema_version)
    column_list = get_column_list(schema_version)

    # prepare output function
    write_func = partial(write_record,
                         profile_schema=profile_schema,
                         skip_writes=skip_writes,
                         stop_on_fail=stop_on_fail)

    # Main processing
    reader = read_files(indir=concat_d)
    reader = starmap(partial(clean_data, column_list=column_list, sample_size=sample_size, ), reader)
    reader = starmap(partial(build_file_events, schema_version=schema_version), reader)
    reader = (write_func(file, record_cnt, idx, record)
              for file, record_cnt, file_records in reader
              for idx, record in file_records)
    outdf = pd.DataFrame.from_records(reader)
    return outdf

    return 'OK'
