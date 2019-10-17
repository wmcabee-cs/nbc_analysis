from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.partition.partition_by_viewer.main import main as partition_by_viewer
from nbc_analysis.utils.debug_utils import retval


def _get_config():
    return get_config(config='default',
                      BATCHES_D='/Users/wmcabee/Dropbox (Cognitive Scale)/NBC Analysis/data/NBC2/batches')


def test_partition_by_viewer():
    config = _get_config()
    week_id = '2019W37'
    return partition_by_viewer(config=config, week_id=week_id)
