import pytest

from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.toml_utils import get_config
from toolz import get_in
from pathlib import Path

TEST_CONFIG = '''
[demographics] 
income_input_f = '{NBC_PROJ_TOP}/datasets/ACS_17_5YR_S2503_with_ann.csv'
'''


def test_text_config():
    config = get_config(config_text=TEST_CONFIG)
    assert isinstance(get_in(['demographics', 'income_input_f'], config), Path)
    return config


def test_test_config():
    config = get_config(config='test')
    assert isinstance(get_in(['demographics', 'income_input_f'], config), Path)
    return config


def test_default_config():
    config = get_config()
    assert isinstance(get_in(['demographics', 'income_input_f'], config), Path)
    return config
