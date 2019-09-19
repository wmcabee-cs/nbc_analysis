import pandas as pd
from toolz import first, take, concat
import arrow
from pathlib import Path
from ..utils.file_utils import init_dir
from ..utils.config_utils import get_config
from ..utils.debug_utils import retval
import shutil
import pprint
import gzip
from subprocess import check_call


def convert_ts(x):
    arr = arrow.get(x)
    return arr.format('YYYY-MM-DD HH:mm:ss')


def proc_group(keys, df, batch_f):
    df = df[~df.event_name.str.startswith('Ad Pod')]
    df = df[~(df.event_name == 'End Card')].copy()
    dx = (df.event_name == 'Video End').astype(int)
    df['video_idx'] = dx.cumsum() - dx
    # print(keys)
    # print(ds)

    for g, dx in df.groupby(['video_idx']):
        ds = dx.iloc[-1][['key', 'idx', 'batch_id', 'mpid', 'session_id', 'customer_id', 'ip',
                          'platform', 'video_type', 'video_end_type', 'show',
                          'season', 'episode_number', 'episode_title',
                          'event_name']].copy()

        ds['start_ts'] = dx['timestamp_unixtime_ms'].min()
        ds['end_ts'] = dx['timestamp_unixtime_ms'].max()

        dt = arrow.get(ds.end_ts / 1000)
        ds['end_utc_day'] = dt.format("YYYYMMDD")
        ds['end_utc_dt'] = dt.format("YYYY-MM-DDTHH:mm:ss")
        ds['video_cnt'] = 1
        ds['ad_watch_cnt'] = (dx.event_name == 'Ad End').sum()
        ds['ad_duration_watched'] = dx.ad_duration_watched.sum()
        ds['duration_watched'] = dx.duration_watched.sum()
        ds['video_cnt'] = 1 if ds.event_name == 'Video End' else 0
        yield ds


def agg_batch(batch_f, outdir):
    print(f">> start aggregation for batch {batch_f}")
    batch_f = Path(batch_f)
    outdir = Path(outdir)

    outfile = outdir / f"agg_{batch_f.name}"

    dirname = outfile.stem.split('.')[0]
    work_d = Path(f"{outdir}") / dirname
    init_dir(work_d, exist_ok=True, rmtree=True)
    tmp_f = work_d / outfile.name

    df = pd.read_csv(batch_f)
    df.sort_values('timestamp_unixtime_ms', inplace=True)
    gb_cols = ['mpid', 'session_id', 'video_id']
    reader = df.groupby(by=gb_cols)
    # reader = take(10, reader)
    reader = (proc_group(keys, df, batch_f) for keys, df in reader)
    reader = concat(reader)
    reader = filter(lambda x: x is not None, reader)
    df = pd.DataFrame.from_records(reader)
    df.to_csv(str(tmp_f), index=False, )
    shutil.move(tmp_f, outfile)
    shutil.rmtree(work_d)
    print(f">> end aggregation, wrote {outfile}, {len(df)}")
    return df


def main():
    config = get_config()

    aggregates_d = Path(config['AGGREGATES_D'])
    platform = Path(config['PLATFORM'])
    infile = aggregates_d / f'aggregates_{platform}.csv'
    df = pd.read_csv(infile)

    outdir = aggregates_d / platform
    init_dir(outdir, exist_ok=True)
    for batch_f in df.batch_f:
        agg_batch(batch_f=batch_f, outdir=outdir)

    return df
