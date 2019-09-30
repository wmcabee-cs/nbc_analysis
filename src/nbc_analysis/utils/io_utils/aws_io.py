from ..aws_utils import get_client, get_bucket
from ..date_utils import parse_day, get_now_zulu
from ..func_utils import take_if_limit
from toolz import first, concat, take, merge
import time
import json
import pprint
from ..debug_utils import retval

import pandas as pd
from pathlib import Path


# TODO: Separate application code from usable component

def list_events_by_day(config, days):
    bucket_key = config['VIDEO_END_BUCKET']
    limit_events_per_batch = config['LIMIT_EVENTS_PER_BATCH']
    bucket = get_bucket(bucket_key)
    extract_spec = 'year={year}/month={month}/event=video_end/NBCUniversal_{day}'

    def list_events(day):
        asof_dt = get_now_zulu()
        day_info = parse_day(day)
        batch_id = f'evnt_vdend_{day}_{asof_dt}'
        prefix = extract_spec.format(day=day, **day_info)
        # print(f">> start list_objects_for_day,day={day},prefix={prefix},asof_dt={asof_dt}")

        reader = bucket.objects.filter(Prefix=prefix).all()
        reader = ((obj.key, obj.size) for obj in reader if obj.key.endswith('.txt'))
        reader = take_if_limit(reader, limit_events_per_batch)
        df = pd.DataFrame.from_records(reader, columns=['name', 'size'])

        # add info to dataframe
        df['batch_id'] = batch_id
        df['asof_dt'] = asof_dt
        # print(f">> end list_objects_for_day,day={day},asof={asof_dt},cnt={len(df)},duration={duration}")
        return (day, asof_dt), df

    return map(list_events, days)


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
