import pytest

from nbc_analysis.utils.config_utils import get_config, get_run_config
from nbc_analysis.runs import init_run, run_batches
from nbc_analysis.utils.debug_utils import retval

TEST_RUN_ID = 'r_testrun'
WEEKS = 10


def _get_config():
    return get_config(config='default')


def _get_run_config():
    return get_run_config(config=_get_config(), run=TEST_RUN_ID)


def test_init_run():
    config = _get_config()
    run_d = init_run(config=config, run_id=TEST_RUN_ID, weeks=WEEKS, exist_ok=True, rmtree=True)
    return run_d


def test_run_batches():
    run_config = _get_run_config()
    run_batches(run_config=run_config, )
