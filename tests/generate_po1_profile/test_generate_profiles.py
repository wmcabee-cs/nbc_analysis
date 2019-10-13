from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.generate_po1_profiles import generate_profiles


def _get_config():
    return get_config(config='default')


def test_generate_profiles():
    config = _get_config()
    return generate_profiles(config)
