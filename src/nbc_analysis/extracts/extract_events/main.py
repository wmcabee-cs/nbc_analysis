from typing import List, Optional
from toolz import take, partial, first, concat, frequencies
from itertools import starmap
import pandas as pd
from pathlib import Path
import json
import gzip
from collections import namedtuple
import pprint

from ...utils.config_utils import get_config
from ...utils.aws_utils import get_bucket
from ...utils.file_utils import init_dir
from ...utils.io_utils.csv_io import read_event_batches
from ...utils.io_utils.aws_io import read_events_in_batch

from ...utils.debug_utils import retval, StopEarlyException
import shutil


def parse_event(batch_id, event):
    try:
        detail = event['events'][0]
        data = detail['data']
        custom_attrs = data['custom_attributes']
        device_current_state = data['device_current_state']
        # $retval(custom_attrs)
        # customer_id = parse_customer_id(user_identities=event.get('user_identities'))
        drec = {
            'batch_id': batch_id,
            'file': event['file'],
            'file_idx': event['file_idx'],
            'asof_dt': event['asof_dt'],

            # USER
            'mpid': event['mpid'],
            'nbc_profile': custom_attrs.get('User Profile', 'None'),
            'mvpd': custom_attrs.get('MVPD', 'None'),
            'profile_status': 'TBD',  # calculated

            # EVENT_TYPE
            'event_name': data['event_name'],
            'event_type': detail['event_type'],

            # PLATFORM
            'platform': event['device_info']['platform'],
            'data_connection_type': device_current_state.get('data_connection_type', 'None'),

            # IP
            'ip': event.get('ip', 'None'),

            # VIDEO
            'video_id': custom_attrs.get('Video ID', 'None'),
            'video_type': custom_attrs.get('Video Type', 'None'),
            'show': custom_attrs.get('Show', 'None'),
            'season': custom_attrs.get('Season', 'None'),
            'episode_number': custom_attrs.get('Episode Number', 'None'),
            'episode_title': custom_attrs.get('Episode Title', 'None'),
            'genre': custom_attrs.get('Genre', None),
            'video_duration_mv': custom_attrs.get('Video Duration'),

            # EVENT_TYPE
            'video_end_type': custom_attrs.get('Video End Type', 'None'),
            'resume': custom_attrs.get('Resume', 'None'),

            # FACT
            'event_id': data['event_id'],
            'event_num': data.get('event_num'),
            'session_id': data['session_id'],
            'video_duration': custom_attrs.get('Video Duration'),
            'video_duration_watched': custom_attrs.get('Duration Watched'),
            'event_start_unixtime_ms': data['event_start_unixtime_ms'],
            'session_start_unixtime_ms': data['session_start_unixtime_ms'],
            'resume_time': custom_attrs.get('Resume Time'),
        }
        return drec
    except StopEarlyException as e:
        raise
    except Exception as e:
        print(f"EROROR: problem parsing event: '{e}'", type(e))
        pprint.pprint(event)
        raise


def parse_events_in_batch(batch_id, reader):
    reader = (parse_event(batch_id=batch_id, event=event) for event in reader)
    return batch_id, reader


def get_partition_writer(partitions_d):
    init_dir(partitions_d, parents=False, exist_ok=True, rmtree=True)

    def write_partition(batch_id, reader):
        outfile = partitions_d / f"{batch_id}.parquet.gz"
        tmp_f = partitions_d / f"_{batch_id}.parquet.gz"
        df = pd.DataFrame.from_records(reader)
        df.to_parquet(tmp_f, index=False)
        shutil.move(tmp_f, outfile)
        print(f">> wrote partition {outfile},row_cnt={len(df)}")

    return write_partition


def main(config_f=None, overrides=None):
    config = get_config(config_f=config_f, overrides=overrides)
    print(f">> start extract events, config={config}")

    batch_limit = config.get('BATCH_LIMIT')
    partitions_d = Path(config['PARTITIONS_D'])
    write_partition = get_partition_writer(partitions_d=partitions_d)

    batches = read_event_batches(config)
    if batch_limit:
        print(f">> WARNING: limiting run to no more than {batch_limit} batches")
        batches = take(batch_limit, batches)

    reader = (read_events_in_batch(config=config, batch_id=batch_id, batch=batch)
              for batch_id, batch
              in batches)

    reader = (parse_events_in_batch(batch_id=batch_id,
                                    reader=rdr)
              for batch_id, rdr in reader)

    reader = (write_partition(batch_id, rdr)
              for batch_id, rdr
              in reader)

    for x in reader:
        pass
    print(">> end proc events")
