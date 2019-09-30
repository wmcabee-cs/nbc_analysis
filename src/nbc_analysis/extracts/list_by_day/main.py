from typing import Dict, Optional, List

from ...utils.config_utils import get_config
from ...utils.io_utils.aws_io import list_events_by_day
from ...utils.io_utils.csv_io import write_event_batches
from nbc_analysis.utils.debug_utils import retval
from toolz import first


def save_locals(local_dict: Dict, exclude):
    return {k.upper(): v
            for k, v in local_dict.items()
            if k not in exclude}


def main(days: List,
         config_f: Optional[str] = None,
         limit_events_per_batch: Optional[int] = None):
    config = get_config(config_f=config_f, overrides={'LIMIT_EVENTS_PER_BATCH': limit_events_per_batch})
    print(f">> extract run, config={config}, day_cnt={len(days)}")

    # Run processing
    reader = list_events_by_day(config, days)
    write_event_batches(config, reader)
    print(">> end extract run")
