import pytest
from nbc_analysis.utils.toml_utils import get_config
from nbc_analysis.demographics import resolve_ip
from nbc_analysis.utils.file_utils import read_parquet
from pathlib import Path


def _get_config():
    return get_config('test')


def test_resolve_ip():
    config = _get_config()
    normalize_d = Path(config['normalize']['normalize_d'])
    ips = read_parquet(indir=normalize_d, name='ips.parquet')
    ip2network = resolve_ip(config=config, ips=ips)
    return ip2network
