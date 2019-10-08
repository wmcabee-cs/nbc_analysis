from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.aws_utils import get_bucket
from pathlib import Path
from toolz import first
import re


def parse_partition_file(infile):
    name = infile.name.split('.')[0]
    date_str = name.split('_')[1]
    reg = r"(?P<year>\d{4})(?P<month>\d\d)(?P<day>\d\d)$"
    m = re.match(reg, date_str)
    date_info = m.groupdict()
    path = "year={year}/month={month}/event=video_end/{name}.parquet".format(name=name, **date_info)
    return infile, path


def upload_partition(bucket, infile, key):
    retval([bucket, infile, key])
    return None


def main(config_f):
    config = get_config(config_f=config_f)
    batches_d = Path(config['BATCHES_D'])
    video_end_partitions_bucket = config['VIDEO_END_PARTITIONS_BUCKET']
    bucket = get_bucket(video_end_partitions_bucket)

    reader = batches_d.glob('*.parquet.gz')
    reader = map(parse_partition_file, reader)
    reader = (upload_partition(bucket, infile, key) for infile, key in reader)

    first(reader)

    retval(batches_d)
