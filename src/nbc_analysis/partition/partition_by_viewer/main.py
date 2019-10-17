from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.utils.aws_utils import get_bucket, download_files
from nbc_analysis.utils.file_utils import init_dir
from nbc_analysis.utils.log_utils import get_logger
from nbc_analysis.utils.func_utils import take_if_limit
from pathlib import Path
from toolz import first, concat
from itertools import groupby

import pandas as pd

RMTREE = True
DOWNLOAD_LIMIT = None

log = get_logger(__name__)


def parse_day(x):
    items = x.name.split('.')[0].split('_')[1]
    return items


def merge_by_day(day, reader):
    reader = (x[1] for x in reader)
    file_list = list(reader)
    file_cnt = len(file_list)
    reader = map(pd.read_parquet, file_list)
    df = pd.concat(reader)
    log.info(f'merged files for day,day={day},files={file_cnt},records={len(df)}')
    return day, df, file_list


def rm_files(file_list):
    for path in file_list:
        path = Path(path)
        if path.is_file():
            log.info(f'deleted file {path}')
            path.unlink()


def deserialize_from_disk(indir):
    log.info(f'start deserialize_from_disk,directory={indir}')
    # deserialize from disk
    reader = indir.glob('*')
    reader = ((parse_day(path), path) for path in reader)
    reader = sorted(reader)
    reader = (merge_by_day(day, files) for day, files in groupby(reader, key=lambda x: x[0]))
    for day, df, file_list in reader:
        rm_files(file_list)
        yield day, df

    return reader


def define_partitions(reader, partition_count):
    def define_partition(day, df):
        df['viewer_partition'] = df.mpid % partition_count
        rdr = ((viewer_partition, day, dx) for viewer_partition, dx in df.groupby('viewer_partition'))
        return rdr

    reader = (define_partition(day, df) for day, df in reader)
    reader = concat(reader)
    return reader


def write_day_partitions(reader, week, outdir):
    def write_partition(partition_num, day, dx):
        partition_id = f'p{partition_num:04d}'
        partition_d = outdir / partition_id
        init_dir(partition_d, exist_ok=True)
        outfile = partition_d / f"ve_{week}_{day}_{partition_id}.parquet"
        dx.to_parquet(outfile, index=False)
        log.info(f"wrote partition file {outfile},records={len(dx)}")

    log.info(f"start write_partition,outdir={outdir}")
    reader = (write_partition(partition_num, day, dx)
              for partition_num, day, dx in reader)
    for x in reader: pass
    log.info(f"end write_partition,outdir={outdir}")


def read_partitions_by_week(indir):
    def merge_files(files):
        reader = (x[1] for x in files)
        file_list = list(reader)
        reader = map(pd.read_parquet, file_list)
        df = pd.concat(reader)
        df = df.sort_values(['mpid', 'event_start_unixtime_ms'])
        df = df.set_index('mpid', drop=False)
        rm_files(file_list)
        return df

    reader = indir.glob('*/*.parquet')

    # partition_id, week, file
    reader = (((x.parent.name, x.name.split('_')[1]), x) for x in reader)
    reader = sorted(reader)
    reader = ((partition_id, week, merge_files(v)) for (partition_id, week), v in
              groupby(reader, key=lambda x: x[0]))

    return reader


def write_week_partitions(reader, outdir):
    def write_partition(partition_id, week, df):
        outfile = outdir / partition_id / f'{partition_id}_{week}.parquet'
        log.info(f"writing partition {outfile},records={len(df)}")
        df.to_parquet(outfile)

    reader = (write_partition(partition_id, week, df) for partition_id, week, df in reader)
    for x in reader: pass


def upload2s3(indir, bucket):
    def upload_batch(infile):
        filename = infile.name
        partition_id = filename.split('_')[0]
        key = f'viewer_partitions/partition={partition_id}/{filename}'
        log.info(f"uploading week partition to s3,key={key} ")
        bucket.upload_file(Filename=str(infile), Key=key)
        return infile

    reader = indir.glob('*/p*.parquet')
    reader = sorted(reader)
    reader = map(upload_batch, reader)
    for infile in reader:
        if infile.is_file():
            log.info(f"deleting file,file={infile}")
            infile.unlink()
        pass


def main(config, week_id):
    prefix = f'clean/year=2019/week={week_id}'

    config = get_config(config=config)
    bucket_name = config['VIEWER_SOURCE_BUCKET']
    viewer_partition_num = config['VIEWER_PARTITION_COUNT']
    work_d = Path(config['WORK_D'])
    partitions_d = work_d / 'partitions'
    batches_d = work_d / 'batches'
    init_dir(work_d, exist_ok=True, parents=True, rmtree=RMTREE)
    init_dir(batches_d, exist_ok=True)
    init_dir(partitions_d, exist_ok=True)

    bucket = get_bucket(bucket_name)
    download_files(bucket=bucket, prefix=prefix, outdir=batches_d, limit=DOWNLOAD_LIMIT)
    reader = deserialize_from_disk(indir=batches_d)
    reader = define_partitions(reader, partition_count=viewer_partition_num)
    write_day_partitions(reader, week=week_id, outdir=partitions_d)

    # combine day partitions into week partitions
    reader = read_partitions_by_week(indir=partitions_d)
    write_week_partitions(reader, outdir=partitions_d)

    # upload to s3
    upload2s3(indir=partitions_d, bucket=bucket)
    log.info(f"end viewer partition for week {week_id}")
    return 'OK'
