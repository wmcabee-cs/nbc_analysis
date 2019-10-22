import pytest
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.toml_utils import get_config
from nbc_analysis.normalize import normalize
from toolz import assoc_in


def _get_config():
    config = get_config('test')
    config = assoc_in(config, ['normalize', 'input_file_limit'], 2)
    return config


def test_normalize():
    config = _get_config()
    return normalize(config)
