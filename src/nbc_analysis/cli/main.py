#!/usr/bin/env python
import click
from pathlib import Path

import nbc_analysis
from nbc_analysis import (get_config, get_run_config, create_day_calendar)

# from nbc_analysis import get_config, extract_file_lists, size_batches, extract_events
# from nbc_analysis.analysis import write_viewer_counts
# from nbc_analysis.utils.file_utils import init_dir
from nbc_analysis.utils.config_utils import write_example_config
import pprint
import pandas as pd


@click.group()
def cli():
    pass


@cli.command(help="write template for configuration file")
@click.option('--outdir', default=".", help="directory to write example configuration")
def example_config(outdir):
    config = write_example_config(outdir)
    pprint.pprint(config)


@cli.command(help="initialize calendar datasets")
def init_calendar():
    config = get_config()
    create_day_calendar(config)


@cli.command(help="initialize a run for specified number of weeks")
@click.option('--weeks', required=True, type=int, help="Number weeks back from start_day to process.")
@click.option('--run_id', help="ID to use for this run. If not specified based on current timestamp.")
@click.option('--start_day', type=int,
              help="Most recent day to process. Default is current day. If specified use format <YYYYMMDD>.")
@click.option('--exist_ok', default=False,
              help="If true continue processing if target directory exists. Default is false.")
@click.option('--rmtree', default=False, help="If true remove target directory if it exists. Default is false.")
def init_run(start_day, weeks, run_id, exist_ok, rmtree):
    config = get_config()
    nbc_analysis.init_run(config=config, run_id=run_id, start_day=start_day, weeks=weeks,
                          exist_ok=exist_ok, rmtree=rmtree)


@cli.command(help="process run")
@click.option('--run_id', required=True, help="Run to process. The run must have already been created using init-run ")
def proc_run(run_id):
    config = get_config()
    run_config = get_run_config(config=config, run=run_id)
    return nbc_analysis.run_batches(run_config=run_config)


if __name__ == '__main__':
    cli()
