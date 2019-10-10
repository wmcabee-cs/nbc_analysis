from nbc_analysis.utils.config_utils import get_week_config
from pathlib import Path
from nbc_analysis.batch import (extract_file_lists, size_batches,
                                extract_events, merge_and_partition, upload_batches)
from nbc_analysis.utils.file_utils import init_dir
import pandas as pd

from nbc_analysis.utils.debug_utils import retval


def proc_week(week_id, days, run_config):
    print(f">> start week processing,week_id={week_id}")
    days = days[days.week_id == week_id].day_key.astype(str)
    days = sorted(days, reverse=True)
    week_config = get_week_config(run_config=run_config, week_id=week_id, days=days)
    week_d = Path(week_config['WEEK_D'])
    init_dir(week_d, exist_ok=True)
    extract_file_lists(week_config=week_config)
    result = size_batches(week_config=week_config)
    if result is None:
        return {'week_id': week_id, 'result': 'empty'}
    extract_events(week_config=week_config)
    merge_and_partition(week_config=week_config)
    upload_batches(week_config=week_config)

    print(f">> end week processing,week_id={week_id}")


def main(run_config):
    run_d = Path(run_config['RUN_D'])
    run_id = run_config['RUN_ID']
    print(f">> start run,run_id={run_id},run_d={run_d}")

    # load weeks
    weeks = pd.read_csv(run_d / 'weeks.csv')
    weeks = weeks.sort_values('week_id', ascending=False)

    # load days
    days = pd.read_csv(run_d / 'days.csv')

    # Run process for each week
    reader = weeks.itertuples()
    reader = (proc_week(rec.week_id, days=days, run_config=run_config) for rec in reader)

    for x in reader: pass
    print(f">> end run,run_id={run_id},run_d={run_d}")
