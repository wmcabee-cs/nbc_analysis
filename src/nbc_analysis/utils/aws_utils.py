import boto3

from pathlib import Path
from nbc_analysis.utils.log_utils import get_logger
from nbc_analysis.utils.func_utils import take_if_limit
from nbc_analysis.utils.debug_utils import retval
from toolz import first

log = get_logger(__name__)


def get_bucket(name):
    s3 = boto3.resource('s3')
    return s3.Bucket(name)


def get_client():
    return boto3.client('s3')


def download_files(bucket, prefix, outdir, limit=None, skip_if_exists=False):
    def download_file(key):
        name = Path(key).name
        outfile = outdir / name
        if outfile.is_file():
            log.info(f"file exists, skipping download,outfile={name}")
            return

        outfile = str(outfile)
        log.debug(f"start s3 download,key={key},outfile={name}")
        bucket.download_file(key, outfile)
        log.info(f"end s3 download,key={key}")

    log.info(f'listing files,prefix={prefix}')
    reader = bucket.objects.filter(Prefix=prefix).all()
    reader = take_if_limit(limit=limit, reader=reader, msg="DOWNLOAD_LIMIT set")
    reader = (download_file(x.key) for x in reader)
    for x in reader: pass
