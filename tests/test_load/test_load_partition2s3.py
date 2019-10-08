import pytest

from nbc_analysis.utils.config_utils import get_config

from nbc_analysis.load.ld_partition2s3.main import main as ld_partition2s3


def _get_test_config():
    return get_config(config_f='default',
                      overrides={
                          'BATCHES_D': '/Users/wmcabee/Dropbox (Cognitive Scale)/NBC Analysis/data/NBC2/batches'})


def test_load_partition2s3():
    return ld_partition2s3(config_f=_get_test_config())

