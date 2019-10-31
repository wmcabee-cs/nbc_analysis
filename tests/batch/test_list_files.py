import s3fs
from pathlib import Path
from nbc_analysis.utils.toml_utils import get_config
from nbc_analysis.batch import extract_file_lists
from toolz import update_in


def _get_config():
    config = get_config('test')
    config['start_day_key'] = 20190730
    config['end_day_key'] = 20190731
    config = update_in(config, ['event_extract', 'file_list_limit'], lambda x: 10)
    return config


def test_list_files():
    config = _get_config()
    return extract_file_lists(config)
