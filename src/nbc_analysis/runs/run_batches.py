from nbc_analysis.utils.config_utils import get_week_config
from pathlib import Path
from nbc_analysis.batch import (extract_file_lists, size_batches,
                                extract_events, merge_and_partition, upload_batches)
from nbc_analysis.utils.file_utils import init_dir
import pandas as pd
import shutil

from nbc_analysis import get_logger, fmt_cfg

log = get_logger(__name__)

from nbc_analysis.utils.debug_utils import retval


def clean_batches(week_config):
    run_id = week_config['RUN_ID']
    week_id = week_config['WEEK_ID']
    log.info(f"start clean_batches,run_id={run_id}week_id={week_id}")

    batches_d = Path(week_config['BATCHES_D'])
    if batches_d.is_dir():
        shutil.rmtree(str(batches_d))

    log.info(f"end clean_batches,run_id={run_id}week_id={week_id}")


def proc_week(week_id, days, run_config):
    try:

        # prepare list of days to process for this week
        days = days[days.week_id == week_id].day_key.astype(str)
        days = sorted(days, reverse=True)

        week_config = get_week_config(run_config=run_config, week_id=week_id, days=days)
        log.info(f"start week,week_id={week_id}")
        week_d = Path(week_config['WEEK_D'])
        init_dir(week_d, exist_ok=True)
        extract_file_lists(week_config=week_config)
        result = size_batches(week_config=week_config)
        if result is None:
            return {'week_id': week_id, 'result': 'empty'}
        extract_events(week_config=week_config)
        upload_batches(week_config=week_config)
        clean_batches(week_config=week_config)
        # merge_and_partition(week_config=week_config)
    except:
        log.exception(f"error week,week_id={week_id}")
    finally:
        log.info(f"end week_id={week_id}")


def main(run_config):
    run_id = run_config['RUN_ID']
    log.info(f"start run,run_id={run_id},run_config={fmt_cfg(run_config)}")
    run_d = Path(run_config['RUN_D'])

    # load weeks
    weeks = pd.read_csv(run_d / 'weeks.csv')
    weeks = weeks.sort_values('week_id', ascending=False)

    # load days
    days = pd.read_csv(run_d / 'days.csv')

    # Run process for each week
    reader = weeks.itertuples()
    reader = (proc_week(rec.week_id, days=days, run_config=run_config) for rec in reader)

    for x in reader: pass
    log.info(f"end run,run_id={run_id}")
