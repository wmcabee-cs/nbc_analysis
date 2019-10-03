#!/usr/bin/env python
import click
from pathlib import Path

from nbc_analysis import get_config, get_test_data, extract_file_lists, size_batches, extract_events
from nbc_analysis.analysis import write_viewer_counts
from nbc_analysis.utils.file_utils import init_dir
import pprint
import pandas as pd


@click.group()
def cli():
    pass


@cli.command(help="show default configuration")
@click.option('--gen_example', default=False, help="View configuration. Generate example configuration")
def config(gen_example):
    click.echo("")
    if gen_example:
        config_f = 'default'
    else:
        config_f = None
    config = get_config(config_f=config_f)
    pprint.pprint(config)


@cli.command(help="generate days dataset to use for testing")
def days():
    days = get_test_data('days.csv')
    config = get_config()
    days_d = Path(config['DAYS_D'])
    init_dir(days_d, exist_ok=True, parents=True)

    outfile = days_d / 'days.csv'
    days.to_csv(outfile, index=False)
    print(f">> wrote file {outfile},records={len(days)}")
    return days


@cli.command(help="extract event file lists")
@click.option('--days_limit', type=int, help="limit number of days in test set. Default is value in configuratin file")
def files(days_limit):
    config = get_config()
    days_d = Path(config['DAYS_D'])
    days_limit = days_limit or config['DAYS_LIMIT']
    infile = days_d / 'days.csv'
    days = pd.read_csv(infile, dtype={'day': str})
    if days_limit is not None:
        days = days.iloc[:days_limit]
    days = days.day.tolist()
    pprint.pprint(days)
    extract_file_lists(days=days, config_f=None)


@cli.command(help="size batches")
def batch_specs():
    size_batches(config_f=None)


@cli.command(help="size batches")
def events():
    extract_events(config_f=None)


@cli.command(help="count unique viewers")
def view_counts():
    write_viewer_counts(config_f=None)


if __name__ == '__main__':
    cli()
