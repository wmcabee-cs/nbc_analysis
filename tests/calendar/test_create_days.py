from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.calendar import create_days


def _get_test_config():
    return get_config(config_f='default',
                      overrides={
                          'BATCHES_D': '/Users/wmcabee/Dropbox (Cognitive Scale)/NBC Analysis/data/NBC2/batches'})


def test_create_days():
    config = _get_test_config()
    return create_days(config_f=config)
