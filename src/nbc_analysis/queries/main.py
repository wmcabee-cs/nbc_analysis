from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.aws_utils import get_bucket, get_client

import boto3


def get_partition(mpid, partition_count):
    partition_num = mpid % 60

    partition_id = f"p{partition_num:40d}"


def main(config, mpid):
    bucket_name = config['VIEWER_SOURCE_BUCKET']
    partition_count = config['VIEWER_PARTITION_COUNT']
    # "viewer_partitions/partition=p00000/p0000_2010W29.parquet"
    prefix = "viewer_partitions/partition={partition}"

    client = get_client()
    retval(config)
