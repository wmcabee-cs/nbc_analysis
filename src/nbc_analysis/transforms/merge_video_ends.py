from nbc_analysis import get_config
from nbc_analysis.utils.file_utils import init_dir
from pathlib import Path

import pandas as pd
from toolz import take
from itertools import starmap
import re
from nbc_analysis.utils.debug_utils import retval


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


def load_files(indir, files):
    print(f">> Loading batches,batch_cnt={len(files)},indir={indir}")
    reader = map(pd.read_parquet, files)
    df = pd.concat(reader)
    print(f">> finished read of batch files,files={len(files)},records={len(df)}")
    return df


def calc_day_range(df):
    print(f">> checking date ranges")
    days = df.day.drop_duplicates()
    start_day = days.min()
    end_day = days.max()
    return start_day, end_day


def add_calculated_fields(df, partition_num):
    print(f">> add calculated fields")
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
        print(f">> wrote viewer_partition {outfile},records={len(df)}")

    init_dir(outdir, exist_ok=True)
    start_day, end_day = calc_day_range(df)
    # initialize partition directories
    print(f">> grouping by partitions")
    reader = df.groupby('viewer_partition_id')
    print(f">> writing partitions,partition_num={partition_num}")
    reader = starmap(write_partition, reader)
    for x in reader: pass


def main(config_f, **overrides):
    config = get_config(config_f=config_f, overrides=overrides)
    indir = Path(config['BATCHES_D'])
    outdir = Path(config['WORK_D'])
    stop_after_merge = config.get('STOP_AFTER_MERGE')
    merge_limit = config.get('BATCH_MERGE_LIMIT', None)
    partition_num = config['VIEWER_PARTITION_NUM']

    files = list_batches(indir=indir, merge_limit=merge_limit)
    df = load_files(indir=indir, files=files)
    df = add_calculated_fields(df, partition_num=partition_num)

    if stop_after_merge is not None:
        return df

    write_partitions(df=df, partition_num=partition_num, outdir=outdir)
    print(f">> nd merge ve_events,records={len(df)}")

    return df
