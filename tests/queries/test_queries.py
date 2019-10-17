import pytest
from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.queries.main import main as query


MPID = []


def _get_config():
    return get_config(config='default')


def test_queries():
    config = _get_config()
    return query(config=config)
