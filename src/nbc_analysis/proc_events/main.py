from toolz import take, partial, first, concat, frequencies
from itertools import starmap
import pandas as pd
from pathlib import Path
import json
import gzip
from collections import namedtuple
import pprint

from ..utils.config_utils import get_config
from ..utils.aws_utils import get_bucket
from ..utils.file_utils import init_dir
from ..utils.debug_utils import retval
import shutil

EVENT_NAMES_TO_KEEP = {
    'Ad End',
    'Ad Pod End',
    'Video Start', 'Video End',
    'End Card',
}
# EVENT_NAMES_TO_KEEP = None

EventRec = namedtuple('EventRec', 'key idx event_type event_name event')


def filter_events(key, idx, event, filename_gz, batch_id):
    event['event_set'] = Path(filename_gz).name
    event['batch_id'] = Path(batch_id).name

    details = event['events']
    if len(details) == 0:
        return None
    detail = details[0]
    event_type = detail['event_type']
    data = detail['data']
    custom_attrs = data['custom_attributes']
    if len(custom_attrs) == 0:
        return None
    event_name = data['event_name']
    if EVENT_NAMES_TO_KEEP is not None:
        if event_name not in EVENT_NAMES_TO_KEEP:
            return None
    # print(event_type)
    return EventRec(key, idx, event_type, event_name, event)


def download_events_file(key, batch_id, bucket, outdir):
    key = Path(key)
    outdir = Path(outdir)
    filename_gz = outdir / key.name
    print(f'>> start download {key}, {filename_gz}')
    if not filename_gz.is_file():
        print(f'>>  - downloading {str(key)}')
        bucket.download_file(str(key), str(filename_gz))
    return str(key), str(filename_gz), batch_id


def read_events(key, filename_gz, batch_id, custom_filter=None, cleanup=False):
    print(f">>opening {filename_gz}")
    with gzip.open(filename_gz, 'r') as f_in:
        reader = enumerate(f_in, start=1)
        reader = (filter_events(key=key,
                                idx=idx,
                                event=json.loads(line),
                                filename_gz=filename_gz,
                                batch_id=batch_id)
                  for idx, line in reader)
        reader = filter(None, reader)
        if custom_filter:
            reader = custom_filter(reader)
        reader = filter(None, reader)

        for x in reader:
            yield x
    # remove file when finished
    if cleanup:
        print(f'>>  - cleaning up {filename_gz}')
        filename_gz = Path(filename_gz)
        filename_gz.unlink()


def default_select_func(event_rec: EventRec, event_cnt: int) -> bool:
    return True, event_cnt


def print_events(reader, select_func=default_select_func):
    event_cnt = 0
    for event_rec in reader:
        print_event, at_event = select_func(event_rec, event_cnt)
        if print_event:
            print('at_event:', at_event, 'event_cnt:', event_cnt)
            if at_event == event_cnt:
                pprint.pprint(event_rec._asdict())
                print('stopping early')
                break
            event_cnt = event_cnt + 1
        yield event_rec


def parse_customer_id(user_identities):
    if user_identities is None or len(user_identities) == 0:
        return 'None'

    if len(user_identities) > 1:
        identity_types = frequencies(x['identity_type'] for x in user_identities)
        if set(identity_types.keys()) != {'customer_id', 'other'}:
            raise Exception("Test case: unexpected identity_type")

    customer_ids = list(x['identity'] for x in user_identities if x['identity_type'] == 'customer_id')
    if len(customer_ids) > 1:
        raise Exception("Test case: multiple customer ids")

    customer_id = 'None' if len(customer_ids) == 0 else customer_ids[0]
    return customer_id


def parse_event(event_rec: EventRec):
    try:
        event = event_rec.event
        detail = event['events'][0]
        event_type = detail['event_type']
        data = detail['data']
        custom_attrs = data['custom_attributes']
        customer_id = parse_customer_id(user_identities=event.get('user_identities'))
        drec = {
            'key': event_rec.key,
            'idx': event_rec.idx,
            'event_set': event['event_set'],
            'batch_id': event['batch_id'],
            'mpid': event['mpid'],
            'event_type': event_type,
            'event_name': data['event_name'],
            'customer_id': customer_id,
            'mvpd': custom_attrs['MVPD'],
            'ip': event['ip'],
            'platform': custom_attrs['Platform'],

            'video_id': custom_attrs.get('Video ID', 'None'),
            'video_type': custom_attrs.get('Video Type', 'None'),
            'video_duration': custom_attrs.get('Video Duration'),
            'show': custom_attrs.get('Show', 'None'),
            'season': custom_attrs.get('Season', 'None'),
            'episode_number': custom_attrs.get('Episode Number', 'None'),
            'episode_title': custom_attrs.get('Episode Title', 'None'),

            'video_end_type': custom_attrs.get('Video End Type', 'None'),

            'event_id': data['event_id'],
            'event_num': data.get('event_num'),
            'session_id': data['session_id'],

            'duration_watched': custom_attrs.get('Duration Watched'),
            'resume': custom_attrs.get('Resume', 'None'),
            'resume_time': custom_attrs.get('Resume Time'),
            'event_start_unixtime_ms': data['event_start_unixtime_ms'],
            'session_start_unixtime_ms': data['session_start_unixtime_ms'],
            'timestamp_unixtime_ms': data['timestamp_unixtime_ms'],

            # Ad end specific fields
            'ad_duration_watched': custom_attrs.get('durationWatched', None),
            'ad_end_type': custom_attrs.get('endType', 'None'),
            'percentage_complete': custom_attrs.get('percentageCompleted', None),

            # Advertising examples
            'campaign_name': custom_attrs.get('campaignName', 'None'),
            'creative_name': custom_attrs.get('creativeName', 'None'),

            # Ad Pod specific fields
            'ad_pod_duration': custom_attrs.get('Ad Pod Duration', None),
            'ad_pod_qty': custom_attrs.get('Ad Pod Quantity', None),
            'ad_pod_type': custom_attrs.get('Ad Pod Type', 'None'),
        }
    except Exception as e:
        print(f"EROROR: problem event: '{e}'", type(e))
        pprint.pprint(event)
        raise
    return drec


def read_batches(indir, platform):
    infile = indir / f'batches_{platform}.csv'

    df = pd.read_csv(infile)
    reader = df[df.extract == platform][['batch_id', 'key', 'extract_f']].groupby(['batch_id'])
    return reader


def proc_batch(batch_id, df, bucket, outdir, event_limit=None):
    work_d = outdir / f'_{batch_id}'
    tmp_f = (work_d / batch_id).with_suffix('.csv')
    outfile = (outdir / batch_id).with_suffix('.csv')

    init_dir(work_d, exist_ok=True)

    reader = zip(df.key, df.batch_id)
    reader = (download_events_file(key=key,
                                   batch_id=batch_id,
                                   bucket=bucket,
                                   outdir=work_d)
              for key, batch_id
              in reader)
    reader = (read_events(key=key,
                          filename_gz=filename_gz,
                          batch_id=batch_id,
                          custom_filter=None,
                          cleanup=True)
              for key, filename_gz, batch_id in reader)
    reader = concat(reader)
    #reader = print_events(reader)  # Prints first event
    reader = print_events(reader, select_func=lambda event_rec, event_cnt: (event_rec.event_name == 'Video Start', 1))
    reader = map(parse_event, reader)  # record from parsed event
    if event_limit:
        reader = take(event_limit, reader)
    df = pd.DataFrame.from_records(reader)
    df.to_csv(tmp_f, index=False)
    shutil.move(tmp_f, outfile)
    shutil.rmtree(work_d)
    return df


def main(config_f=None):
    # get inputs from config file
    print(">> init proc events")
    config = get_config(config_f)
    batch_limit = config.get('BATCH_LIMIT')
    event_limit = config.get('EVENT_LIMIT')
    platform = 'android'

    print(f">> start proc run, config={config}")
    batches_d = Path(config['BATCHES_D'])
    bucket = get_bucket(config['RAW_EVENTS_BUCKET'])

    outdir = batches_d / platform
    init_dir(outdir, exist_ok=True)

    # Read batch files
    reader = read_batches(indir=batches_d, platform=platform)
    if batch_limit:
        reader = take(batch_limit, reader)

    # Read batch files
    reader = (proc_batch(batch_id, df, bucket, outdir=outdir, event_limit=event_limit) for batch_id, df in reader)
    for x in reader:
        pass
    print(">> end proc events")
