import s3fs
from pathlib import Path
from nbc_analysis.utils.toml_utils import get_config


def _get_config():
    return get_config('test')


def test_list_files():
    config = _get_config()
    fs = s3fs.S3FileSystem()
    fs.ls
    bucket = Path('nbc-digital-cloned')
    return fs
