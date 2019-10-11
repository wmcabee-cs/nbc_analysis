from nbc_analysis import get_config
from nbc_analysis.utils.file_utils import init_dir
from pathlib import Path

import pandas as pd
from toolz import take
from itertools import starmap
import re
from nbc_analysis.utils.debug_utils import retval

from ...utils.log_utils import get_logger

log = get_logger(__name__)


# TODO: Add day in input dataset when batch id is created
# TODO: make sure days are contiguous
# TODO: pass in start end days into this step, then verify against days in dataaset


def calc_partition_id(df, partition_num):
    return (df.mpid % partition_num).map("p{:03d}".format)


pattern = '(?P<event>[^]_]+)_(?P<day>\d{4}\d{2}\d{2})_\d+$'
# pattern =  've_(?P<year>\d{4}(?P<month>\d{2})(?P<day>\d{2})_)'
regex = re.compile(pattern)


def parse_batch_id(batch_id):
    m = regex.match(batch_id)
    if m is None:
        return None
    return m['day']


def list_batches(indir, merge_limit):
    reader = indir.glob('*')
    reader = take(merge_limit, reader) if merge_limit is not None else reader
    files = sorted(reader)
    return files


def merge_files(indir, files):
    log.info(f"start merge_batches,batch_cnt={len(files)},indir={indir}")
    reader = map(pd.read_parquet, files)
    df = pd.concat(reader)
    log.info(f"finish merge_batches,merged_cnt={len(df)}")
    return df


def calc_day_range(df):
    log.debug(f"start check_date_ranges")
    days = df.day.drop_duplicates()
    start_day = days.min()
    end_day = days.max()
    return start_day, end_day


def add_calculated_fields(df, partition_num):
    log.debug(f"start add_calculated_fields")
    df['viewer_partition_id'] = calc_partition_id(df, partition_num=partition_num)
    df['day'] = df.batch_id.map(parse_batch_id)
    return df


def write_partitions(df, partition_num, outdir):
    def write_partition(viewer_partition_id, partition_df):
        partition_d = outdir / viewer_partition_id
        outfile = partition_d / f've_{viewer_partition_id}_{start_day}_{end_day}.parquet'

        # Initialize directories if first write
        init_dir(partition_d, parents=True, exist_ok=True)

        # sort dataset
        scols = ['viewer_partition_id', 'mpid', 'event_start_unixtime_ms']
        df = partition_df.sort_values(scols)

        # write partition to disk
        df.to_parquet(outfile, index=False)
        log.info(f"wrote viewer_partition,partition={outfile.name},records={len(df)}")

    log.debug(f"start write_partitions,partition_num={partition_num}")
    init_dir(outdir, exist_ok=True)
    start_day, end_day = calc_day_range(df)
    # initialize partition directories
    log.debug(f"grouping by partitions")
    reader = df.groupby('viewer_partition_id')
    reader = starmap(write_partition, reader)
    for x in reader: pass


def main(week_config, stop_after_merge=None):
    week_id = week_config['WEEK_ID']
    run_id = week_config['RUN_ID']
    log.info(f"start merge_and_partition,run_id={run_id},week_id={week_id}")

    indir = Path(week_config['BATCHES_D'])
    outdir = Path(week_config['VIEWER_PARTITION_D'])
    partition_num = week_config['VIEWER_PARTITION_NUM']

    merge_limit = week_config.get('BATCH_MERGE_LIMIT', None)

    files = list_batches(indir=indir, merge_limit=merge_limit)
    df = merge_files(indir=indir, files=files)
    df = add_calculated_fields(df, partition_num=partition_num)

    if stop_after_merge:
        return df

    write_partitions(df=df, partition_num=partition_num, outdir=outdir)
    log.info(f"end merge_and_partition,run_id={run_id},week_id={week_id}")

    return df
