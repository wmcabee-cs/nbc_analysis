from ..aws_utils import get_client, get_bucket
from ..date_utils import parse_day, get_now_zulu
from ..func_utils import take_if_limit
from toolz import first, concat, take
import time
import json
from ..debug_utils import retval

import pandas as pd
from pathlib import Path


def list_events_by_day(config, days):
    bucket_key = config['VIDEO_END_BUCKET']
    limit_events_per_batch = config['LIMIT_EVENTS_PER_BATCH']
    bucket = get_bucket(bucket_key)
    extract_spec = 'year={year}/month={month}/event=video_end/NBCUniversal_{day}'

    def list_events(day):
        asof_dt = get_now_zulu()
        day_info = parse_day(day)
        prefix = extract_spec.format(day=day, **day_info)
        # print(f">> start list_objects_for_day,day={day},prefix={prefix},asof_dt={asof_dt}")

        start = time.time()
        reader = bucket.objects.filter(Prefix=prefix).all()
        reader = take_if_limit(reader, limit_events_per_batch)
        reader = ((obj.key, obj.size) for obj in reader)
        df = pd.DataFrame.from_records(reader, columns=['name', 'size'])
        duration = round(time.time() - start, 5)

        # add info to dataframe
        df['batch_id'] = day
        df['asof_dt'] = asof_dt
        # print(f">> end list_objects_for_day,day={day},asof={asof_dt},cnt={len(df)},duration={duration}")
        return (day, asof_dt), df

    return map(list_events, days)


def read_events_in_batch(config, path, batch):
    bucket = config['VIDEO_END_BUCKET']
    first_events = 10 #config['FIRST_EVENTS']
    s3 = get_client()

    print(f'>> start event download,batch={path}')

    def download_events(name):
        print(f">>downloading {name}")

        retr = s3.get_object(Bucket=bucket, Key=str(name))
        reader = retr['Body'].iter_lines()
        reader = map(json.loads, reader)
        return reader

    reader = map(download_events, batch.name)
    reader = concat(reader)
    reader = take(first_events, reader)
    return path, reader
