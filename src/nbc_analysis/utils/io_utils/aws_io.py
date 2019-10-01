from ..aws_utils import get_client, get_bucket
from ..date_utils import parse_day, get_now_zulu
from ..func_utils import take_if_limit
from toolz import first, concat, take, merge
from itertools import starmap
import time
import json
import pprint
from ..debug_utils import retval

import pandas as pd
import numpy as np
from pathlib import Path


# TODO: Separate application code from usable component

def list_files_by_day(config, days):
    bucket_key = config['VIDEO_END_BUCKET']

    # for testing. Only pull N number of files per pattern
    limit_files_per_day = config.get('LIMIT_FILES_PER_DAY')
    bucket = get_bucket(bucket_key)
    pattern_prefix = Path('year={year}/month={month}/event=video_end')
    extract_specs = {
        'ios': 'NBCUniversal_',
        'android': 'NBC_',  # android, roku, tvOS
        'web': 'NBC_App_',
    }

    def get_files(pattern_type, prefix, day):
        # print(f">> start get_files,pattern_type={pattern_type},prefix={prefix},day={day}")
        prefix_w_day = prefix + day
        reader = bucket.objects.filter(Prefix=prefix_w_day).all()
        reader = ((obj.key, obj.size) for obj in reader if obj.key.endswith('.txt'))
        reader = take_if_limit(reader, limit_files_per_day)
        df = pd.DataFrame.from_records(reader, columns=['file', 'size'])
        df['pattern_type'] = pattern_type
        regex = prefix + r'(\d+)_.+.txt'
        df['file_dt'] = df.file.str.extract(regex, expand=False)
        df['file_dt'] = df['file_dt']
        # print(f">> end get_files,pattern_type={pattern_type},prefix={prefix}")
        return df

    def list_files(day):
        asof_dt = get_now_zulu()
        day_info = parse_day(day)
        reader = ((pattern_type, str(pattern_prefix / f'{spec}'))
                  for pattern_type, spec in extract_specs.items())
        reader = (get_files(pattern_type=pattern_type,
                            prefix=prefix_tmpl.format(**day_info),
                            day=day)
                  for pattern_type, prefix_tmpl in reader)
        df = pd.concat(reader)

        df['day'] = day
        df['asof_dt'] = asof_dt
        #print(f">> end list_objects_for_day,day={day},asof={asof_dt},cnt={len(df)}")
        return day, df

    return map(list_files, days)


def safe_json_loads(line):
    try:
        return json.loads(line)
    except Exception as e:
        print(">> ERROR: during json parse,problem line :")
        pprint.pprint(line)
        raise


# TODO: Separate application code from usable component
# TODO: Logging too much per batch.  Add init section for one time logging
def read_events_in_batch(config, batch_id, batch):
    first_rec = batch.iloc[0]
    asof_dt = first_rec.asof_dt
    bucket = config['VIDEO_END_BUCKET']
    print(f'>> start event download,batch_id={batch_id}')
    limit_events_per_batch = config.get("LIMIT_EVENTS_PER_BATCH")
    if limit_events_per_batch is None:
        print(f">> WARNING: Limiting events to no more than {limit_events_per_batch} events per batch")
    s3 = get_client()

    def download_events(name):
        print(f">>downloading {name}")

        filename = Path(name).name

        retr = s3.get_object(Bucket=bucket, Key=str(name))
        reader = retr['Body'].iter_lines()
        reader = map(safe_json_loads, reader)
        reader = (merge(x, {'file_idx': file_idx, 'file': filename, 'asof_dt': asof_dt})
                  for file_idx, x in enumerate(reader))
        return reader

    reader = map(download_events, batch.name)
    reader = concat(reader)
    if limit_events_per_batch is not None:
        reader = take(limit_events_per_batch, reader)

    return batch_id, reader
