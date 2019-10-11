from toolz import take
import pandas as pd
from pathlib import Path
import pprint

from nbc_analysis.utils.file_utils import init_dir
from nbc_analysis.utils.io_utils.csv_io import read_event_batches
from nbc_analysis.utils.io_utils.aws_io import read_events_in_batch
from nbc_analysis.utils.debug_utils import retval, StopEarlyException
import shutil

from ...utils.log_utils import get_logger

log = get_logger(__name__)

EMPTY_DICT = {}

MISSING = 'Not Set'


def replace_none(astr):
    if astr == 'None':
        return None
    return astr


def parse_event(batch, event):
    try:
        detail = event['events'][0]
        data = detail['data']
        custom_attrs = data['custom_attributes']
        user_attrs = event.get('user_attributes', EMPTY_DICT)
        device_current_state = data.get('device_current_state', EMPTY_DICT)
        drec = {
            'batch_id': batch.batch_id,
            'file': event['file'],
            'file_idx': event['file_idx'],
            'asof_dt': event['asof_dt'],

            # USER
            'mpid': event['mpid'],
            'nbc_profile': user_attrs.get('User Profile', MISSING),
            'mvpd': custom_attrs.get('MVPD', MISSING),

            # EVENT_TYPE
            'event_name': data['event_name'],
            'event_type': detail['event_type'],

            # PLATFORM
            'platform': event['device_info'].get('platform', MISSING),
            'data_connection_type': device_current_state.get('data_connection_type', MISSING),

            # IP
            'ip': event.get('ip', MISSING),

            # VIDEO
            'video_id': custom_attrs.get('Video ID', MISSING),
            'video_type': custom_attrs.get('Video Type', MISSING),
            'show': custom_attrs.get('Show', MISSING),
            'season': custom_attrs.get('Season', MISSING),
            'episode_number': custom_attrs.get('Episode Number', MISSING),
            'episode_title': custom_attrs.get('Episode Title', MISSING),
            'genre': custom_attrs.get('Genre', MISSING),
            'video_duration': replace_none(custom_attrs.get('Video Duration')),

            # EVENT_TYPE
            'video_end_type': custom_attrs.get('Video End Type', MISSING),
            'resume': custom_attrs.get('Resume', MISSING),

            # FACT
            'event_id': data['event_id'],
            'session_id': data.get('session_id', MISSING),
            'video_duration_watched': custom_attrs.get('Duration Watched'),
            'event_start_unixtime_ms': data['event_start_unixtime_ms'],
            'session_start_unixtime_ms': data.get('session_start_unixtime_ms', None),
            'resume_time': custom_attrs.get('Resume Time', None),
        }
        return drec
    except StopEarlyException as e:
        raise
    except Exception as e:
        log.exception(f"problem parsing event")
        log.error(pprint.pformat(event))
        raise


fill_null_fields = ['genre', 'video_type', 'episode_number', 'episode_title']


def parse_events_in_batch(batch, reader):
    reader = (parse_event(batch=batch, event=event) for event in reader)
    df = pd.DataFrame.from_records(reader)
    df[fill_null_fields] = df[fill_null_fields].fillna(MISSING)
    return batch, df


def get_batch_writer(batches_d):
    init_dir(batches_d, parents=False, exist_ok=True, rmtree=False)

    def write_batch(batch, df):
        batch_id = batch.batch_id
        outfile = batches_d / f"{batch_id}.parquet.gz"
        tmp_f = batches_d / f"_{batch_id}.parquet.gz"

        df.to_parquet(tmp_f, index=False)
        shutil.move(tmp_f, outfile)
        log.info(f"wrote partition {outfile},records={len(df)}")

    return write_batch


def main(week_config):
    run_id = week_config['RUN_ID']
    week_id = week_config['WEEK_ID']
    log.info(f"start extract_events,run_id={run_id},week_id={week_id}")

    batch_spec_d = Path(week_config['BATCH_SPEC_D'])
    batches_d = Path(week_config['BATCHES_D'])
    bucket = week_config['VIDEO_END_BUCKET']

    batch_limit = week_config.get('BATCH_LIMIT')
    batch_files_limit = week_config.get('BATCH_FILES_LIMIT')

    # setup IO
    write_batches = get_batch_writer(batches_d=batches_d)

    reader = read_event_batches(batch_spec_d=batch_spec_d, batch_limit=batch_limit, batch_files_limit=batch_files_limit)
    reader = (read_events_in_batch(bucket=bucket, batch=batch, files=files) for batch, files in reader)
    reader = (parse_events_in_batch(batch=batch, reader=rdr) for batch, rdr in reader)

    reader = (write_batches(batch, rdr) for batch, rdr in reader)

    for x in reader:
        pass
    log.info(f"end events_extract,run_id={run_id},week_id={week_id}")
