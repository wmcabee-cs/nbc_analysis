from ..utils.file_utils import init_dir
from ..utils.config_utils import get_config
import itertools
from toolz import take, partial
import pandas as pd
import boto3


def get_bucket(name):
    s3 = boto3.resource('s3')
    return s3.Bucket(name)


def write_sample(extract, spec, day, bucket, workdir, limit):
    prefix = spec['prefix'].format(day=day)
    extract_f = f"{extract}_{day}.csv"
    outfile = workdir / extract_f
    print(f">> start extract,extract={extract},outfile={outfile}")

    reader = bucket.objects.filter(Prefix=prefix).all()
    reader = take(limit, reader)
    reader = ((x.key, x.size) for x in reader)
    df = pd.DataFrame.from_records(reader, columns=['name', 'size'])
    df['day'] = day
    df['extract'] = extract
    df['extract_f'] = extract_f
    df.to_csv(outfile, index=False)
    df = df.sort_values(['size'])
    print(f">> end extract,extract={extract},outfile={outfile},records={len(df)}")
    return df


def main(config_f=None):
    # get inputs from config file
    print(">> init extract run")
    config = get_config(config_f)

    print(f">> start extract run, config={config}")
    event_set_d = config['EVENT_SET_D']
    extract_specs = config['EXTRACT_SPECS']
    days = config['DAYS']
    limit = config['LIMIT']

    bucket = get_bucket(config['RAW_EVENTS_BUCKET'])

    workdir = init_dir(event_set_d, parents=True, exist_ok=True, rmtree=False)
    reader = iter(extract_specs.items())
    reader = ((extract, spec, day) for extract, spec in reader for day in days)
    reader = itertools.starmap(partial(write_sample, workdir=workdir, bucket=bucket, limit=limit), reader)
    for x in reader: pass
    print(">> end extract run")
