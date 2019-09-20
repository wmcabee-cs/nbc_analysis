# -*- coding: utf-8 -*-
from pkg_resources import get_distribution, DistributionNotFound

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = get_distribution(dist_name).version
except DistributionNotFound:
    __version__ = 'unknown'
finally:
    del get_distribution, DistributionNotFound

# import to top namespace
from .extracts.main import main as run_extracts
from .proc_events.main import main as proc_events
from .size_batches.main import main as size_batches
from .agg_video_end.main import main as agg_video_end
from .build_aggregate_run.main import main as build_aggregate_run
from .concat_filtered_events.main import main as concat_filtered_events
