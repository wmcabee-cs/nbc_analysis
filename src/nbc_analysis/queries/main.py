from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.aws_utils import get_bucket, get_client, download_files
from nbc_analysis.utils.file_utils import init_dir

import boto3
from pathlib import Path


def get_partition(mpid, partition_count):
    partition_num = mpid % 60

    partition_id = f"p{partition_num:40d}"


def download_test_files(config, partition, limit=None):
    """ Pull locally to find test dataserts"""
    bucket_name = config['VIEWER_SOURCE_BUCKET']
    outdir = Path(config['WORK_D'], 'query')
    init_dir(outdir, exist_ok=True, parents=True, rmtree=True)
    prefix = f"viewer_partitions/partition={partition}"

    bucket = get_bucket(bucket_name)
    download_files(bucket=bucket, prefix=prefix, outdir=outdir, limit=limit)


def main(config, mpid):
    partition_count = config['VIEWER_PARTITION_COUNT']
    # "viewer_partitions/partition=p00000/p0000_2010W29.parquet"

    # client = get_client()

    retval(config)
