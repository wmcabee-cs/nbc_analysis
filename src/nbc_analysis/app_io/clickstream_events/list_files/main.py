from typing import Dict

from nbc_analysis.utils.io_utils.aws_io import list_files_by_day
from nbc_analysis.utils.io_utils.csv_io import write_event_batches, write_file_lists
from nbc_analysis.utils.log_utils import get_logger
from nbc_analysis.utils.func_utils import take_if_limit
from toolz import concat, first
import pandas as pd
from cortex_io.aws.s3 import get_matching_s3_objects

from nbc_analysis.utils.debug_utils import retval
from pathlib import Path

log = get_logger(__name__)

PATTERNS = [
    # 'NBCProd/Android/NBC_{day}',
    # 'NBCProd/Roku/NBC_{day}',
    'NBCProd/Web/NBC_App_{day}',
    'NBCProd/iOS/NBC_{day}',
]


def get_prefixes(days):
    reader = (pattern.format(day=day)
              for day in days
              for pattern in PATTERNS)
    return reader


def get_days_to_process(start_day_key, end_day_key):
    return iter([start_day_key, end_day_key])


def list_files(bucket, prefixes):
    reader = prefixes
    reader = (get_matching_s3_objects(bucket=bucket, prefix=prefix) for prefix in reader)
    reader = concat(reader)
    for obj in reader:
        yield obj['Key'], obj['Size']


def main(config, start_day_key, end_day_key):
    cfg = config['event_extract']
    bucket = cfg['bucket']
    extract_file_limit = cfg.get('file_list_limit')

    days = get_days_to_process(start_day_key, end_day_key)
    prefixes = get_prefixes(days)
    reader = list_files(bucket=bucket, prefixes=prefixes)
    reader = take_if_limit(reader, limit=extract_file_limit, msg="extract_file_limit set")
    files = pd.DataFrame(reader, columns=['path', 'size'])
    log.info(f"end extract_file_lists")
    return files
