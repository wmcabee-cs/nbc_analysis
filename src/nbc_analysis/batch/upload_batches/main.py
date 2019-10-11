from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.aws_utils import get_bucket
from pathlib import Path
from toolz import first
import re
from ...utils.log_utils import get_logger

log = get_logger(__name__)


def parse_batch_file(week_id, infile):
    name = infile.name.split('.')[0]
    date_str = name.split('_')[1]
    reg = r"(?P<year>\d{4})(?P<month>\d\d)(?P<day>\d\d)$"
    m = re.match(reg, date_str)
    date_info = m.groupdict()
    path = "clean/year={year}/week={week_id}/{name}.parquet".format(name=name, week_id=week_id, **date_info)
    return infile, path


def upload_batch(bucket, infile, key):
    log.info(f"upload batch s3,dest={key} ")
    bucket.upload_file(Filename=str(infile), Key=key)
    return None


def main(week_config):
    week_id = week_config['WEEK_ID']
    run_id = week_config['RUN_ID']

    log.info(f"start upload_batches,run_id={run_id},week_id={week_id}")
    batches_d = Path(week_config['BATCHES_D'])
    video_end_partitions_bucket = week_config['VIDEO_END_PARTITIONS_BUCKET']
    bucket = get_bucket(video_end_partitions_bucket)

    reader = batches_d.glob('*.parquet.gz')
    reader = (parse_batch_file(week_id=week_id, infile=infile) for infile in reader)
    reader = (upload_batch(bucket=bucket, infile=infile, key=key) for infile, key in reader)
    for x in reader: pass
    log.info(f"end upload_batches,run_id={run_id},week_id={week_id}")
