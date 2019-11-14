import pytest
from nbc_analysis.utils.toml_utils import get_config
from nbc_analysis.queries import query, query_detail, get_db

MULTI_GENRES_MPID = 594429747067128960
TRAVELER_MPID = 2882228801219022660


def _get_config():
    return get_config(config='test')


def test_queries_multi_genre():
    mpid = MULTI_GENRES_MPID
    config = _get_config()
    return query(config=config, mpid=mpid)


def test_queries_traveler():
    mpid = TRAVELER_MPID
    config = _get_config()
    return query(config=config, mpid=mpid)


def test_query_detail():
    mpid = MULTI_GENRES_MPID
    # mpid = TRAVELER_MPID
    config = _get_config()
    return query_detail(config=config, mpid=mpid)


def test_get_engine():
    config = _get_config()
    cfg = config['database']
    return get_db(cfg=cfg)
