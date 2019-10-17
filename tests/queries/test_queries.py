import pytest
from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.queries.main import main as query, download_test_files

MPID = []
PARTITION_ID = 'p0000'


def _get_config():
    return get_config(config='default')


def test_download_files():
    config = get_config(config='default')
    download_test_files(config, partition=PARTITION_ID,limit=None)


def test_queries():
    config = _get_config()
    return query(config=config)
