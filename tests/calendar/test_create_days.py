from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.calendar import create_days


def _get_test_config():
    return get_config(config='default')


def test_create_days():
    config = _get_test_config()
    return create_days(config=config)
