import pandas as pd
import numpy as np
from toolz import partial
from nbc_analysis.utils import dim_utils
from nbc_analysis.utils.log_utils import get_logger
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.file_utils import init_dir, write_parquet, read_parquet_dir
from nbc_analysis.demographics import resolve_ip

log = get_logger(__name__)


def get_dim_funcs():
    upd_ts = '_last_upd_dt'

    # Use helper utilities to build dimension transforms.
    #   Each will return dim
    #   WARNING: data is potentially modified in place

    dim_funcs = dict(
        dim_video=partial(dim_utils.build_nk_dim,
                          nk='video_id',
                          pk='video_key',
                          upd_ts=upd_ts,
                          cols=['video_type', 'show', 'season', 'episode_number', 'episode_title', 'genre',
                                'video_duration']),
        dim_event_type=partial(dim_utils.build_hash_dim,
                               hash_code='event_type_key',
                               upd_ts=upd_ts,
                               cols=['event_name', 'event_type']),
        dim_platform=partial(dim_utils.build_hash_dim,
                             hash_code='platform_key',
                             upd_ts=upd_ts,
                             cols=['platform', 'data_connection_type']),
        dim_profile=partial(dim_utils.build_hash_dim,
                            hash_code='profile_key',
                            upd_ts=upd_ts,
                            cols=['mvpd', 'nbc_profile']),
        dim_end_type=partial(dim_utils.build_hash_dim,
                             hash_code='end_type_key',
                             upd_ts=upd_ts,
                             cols=['video_end_type', 'resume']),
    )
    return dim_funcs


def clean_input(df):
    log.info('start clean input')
    treat_as_null = {'None', 'Not Set', '', None}
    df.drop(columns=['session_start_unixtime_ms', 'event_id',
                     'session_id', 'asof_dt', ], inplace=True)
    df['_last_upd_dt'] = df.pop('event_start_unixtime_ms').astype(np.int)
    df['_file'] = df.pop('file')
    df['_file_idx'] = df.pop('file_idx')
    df['_batch_id'] = df.pop('batch_id')
    df['_viewer_partition'] = df.pop('viewer_partition')

    log.info('event standardize null representations')
    df = df.mask(df.isin(treat_as_null))  # Replace all null representations with np.NaN
    log.info('event filter video id')
    df = df[df.video_id.notnull()].copy()
    log.info('event convert measures to float')
    df['resume_time'] = df.resume_time.astype(np.float)
    df['video_duration_watched'] = df.video_duration_watched.astype(np.float)
    df['video_duration'] = df.video_duration_watched.astype(np.float)

    log.info('event fill string nulls')
    fillna_cols = ['resume', 'video_end_type', 'genre', 'episode_title', 'episode_number', 'season',
                   'show', 'video_type', 'data_connection_type', 'ip', 'platform', 'event_type', 'event_name',
                   'mvpd', 'nbc_profile']
    df[fillna_cols] = df[fillna_cols].fillna('Not Set')
    assert df.video_id.isnull().sum() == 0, "Null values in video ID"
    log.info('end clean input')

    return df


def set_ts_fields(data):
    log.info("start derive timestamp information")
    log.info("event convert _last_upd_dt to date time")
    last_upd_dt = data['_last_upd_dt']
    ts = last_upd_dt / 1000
    ts = pd.to_datetime(ts, unit='s', origin='unix')

    # TODO: Add arrival date from timestamps in source files
    log.info("event format event_start_dt")
    data['event_start_dt'] = ts.dt.strftime('%Y%m%dT%H%M%S.%fZ')
    log.info("event format date_utc_key")
    data['day_utc_key'] = ts.dt.strftime('%Y%m%d').astype(np.int)
    log.info("event dedup date keys")
    dim = data[['day_utc_key']].drop_duplicates()
    log.info("end derive timestamp information")
    return dim


def get_fact(df):
    to_cols = [
        'day_utc_key',
        'video_key',
        'platform_key',
        'profile_key',
        'end_type_key',
        'event_type_key',
        'network_key',
        'mpid',
        'event_start_dt',
        'video_duration_watched',
        'resume_time',
        '_file',
        '_file_idx',
        '_batch_id',
        '_viewer_partition',
        '_last_upd_dt',
    ]
    mismatch = set(df.columns).symmetric_difference(set(to_cols))
    if mismatch:
        raise Exception(f"column format does not match expected, {mismatch}")
    df = df.reindex(columns=to_cols)
    df = df.reset_index(drop=True)
    return df


def add_network_info(config, df, outdir):
    log.info("start add network info")
    ips = dim_utils.build_unique_set(data=df, cols=['ip'])
    ips = write_parquet(name='_ips', df=ips, outdir=outdir)

    log.info('event resolve_ip')
    ip2network = resolve_ip(config=config, ips=ips)

    log.info('event broadcast network_key to fact')
    lkup = ip2network.set_index('ip')
    df['network_key'] = df.ip.map(lkup.network_key)
    log.info("end add network info")

    df.drop(columns=['ip'], inplace=True)

    return df


def main(config):
    # !! SIDE EFFECT WARNING: DATA is modified in place throughout this function

    cfg = config['normalize']
    log.info(f'start normalize video end, {cfg}')

    # define io
    indir = init_dir(cfg['test_input_d'], exist_ok=True)
    outdir = init_dir(cfg['normalize_d'], exist_ok=True, rmtree=True)
    file_limit = cfg.get('input_file_limit', None)

    # read input
    df = read_parquet_dir(indir=indir, limit=file_limit, msg='file limit set on normalize read')
    log.info(f"start sort by event timestamp")
    df = df.sort_values('event_start_unixtime_ms', ascending=False).reset_index(drop=True)
    log.info(f"end sort by event timestamp")

    # preprocess fact and timestamp fields
    df = clean_input(df)

    # prepare day_key and timestamps
    day_utc_keys = set_ts_fields(df)
    write_parquet(name='_day_utc_keys', df=day_utc_keys, outdir=outdir)

    # prepare day_key and timestamps
    df = add_network_info(config=config, df=df, outdir=outdir)

    # dim_funcs modifieds data in place
    log.info("start build dimensions")
    dim_funcs = get_dim_funcs()
    reader = dim_funcs.items()
    reader = ((dim_name, dim_func(df)) for dim_name, dim_func in reader)
    for dim, dim_df in reader:
        write_parquet(name=dim, df=dim_df, outdir=outdir)
    log.info("end build dimensions")

    # write fact to outfile
    fact = get_fact(df)
    fact = write_parquet(name='f_video_end', df=fact, outdir=outdir)
    log.info('end normalize video end')
    return fact
