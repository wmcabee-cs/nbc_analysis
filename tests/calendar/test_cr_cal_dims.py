from nbc_analysis.utils.toml_utils import get_config
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.calendar import cr_cal_dims


def _get_config():
    return get_config('test')


def test_cr_cal_dims():
    config = _get_config()
    return cr_cal_dims(config=config)
