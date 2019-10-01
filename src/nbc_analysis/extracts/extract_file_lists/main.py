from typing import Optional, List

from ...utils.config_utils import get_config
from ...utils.io_utils.aws_io import list_files_by_day
from ...utils.io_utils.csv_io import write_event_batches, write_file_lists
from nbc_analysis.utils.debug_utils import retval
from toolz import first
from itertools import starmap


def main(days: List, config_f: Optional[str] = None):
    config = get_config(config_f=config_f)
    print(f">> start extract_file_lists , config={config}, day_cnt={len(days)}")

    # Run processing
    reader = list_files_by_day(config, days)
    write_file_lists(config, reader)
    print(f">> end extract_file_lists")
