from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.partition.partition_by_viewer.main import main as partition_by_viewer


def _get_test_config():
    return get_config(config_f='default',
                      overrides={
                          'BATCHES_D': '/Users/wmcabee/Dropbox (Cognitive Scale)/NBC Analysis/data/NBC2/batches'})


def test_partition_by_viewer():
    config = _get_test_config()
    return partition_by_viewer(config_f=config)
    retval(config)
