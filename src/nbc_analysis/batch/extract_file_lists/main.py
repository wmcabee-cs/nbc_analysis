from typing import Dict

from nbc_analysis.utils.io_utils.aws_io import list_files_by_day
from nbc_analysis.utils.io_utils.csv_io import write_event_batches, write_file_lists
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.log_utils import get_logger

log = get_logger(__name__)


def main(week_config):
    run_id = week_config['RUN_ID']
    week_id = week_config['WEEK_ID']
    days = week_config['DAYS']
    log.info(f"start extract_file_lists,run_id={run_id},week_id={week_id},day_cnt={len(days)}")

    # Run processing
    reader = list_files_by_day(week_config=week_config, days=days)
    write_file_lists(week_config=week_config, reader=reader)
    log.info(f"end extract_file_lists")
