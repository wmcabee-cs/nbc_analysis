from nbc_analysis.utils.toml_utils import get_config
from nbc_analysis.load_db import load_db, initialize_db
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.file_utils import init_dir


def _get_config():
    config = get_config('test')
    return config


def test_load_db():
    config = _get_config()
    return load_db(config=config)


def test_initialize_db():
    config = _get_config()
    return initialize_db(config['database'])
