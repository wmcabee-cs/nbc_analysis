from nbc_analysis.utils.log_utils import get_logger
from nbc_analysis.utils.file_utils import init_dir, write_parquet
from nbc_analysis.utils.debug_utils import retval

from .prep_zip2income.main import main as prep_zip2income
from .prep_subnet2zip.main import main as prep_subnet2zip

from pandas.util import hash_pandas_object
import pandas as pd

log = get_logger(__name__)


def merge_datasets(zip2inc, subnet2inc, name):
    log.info(f'start merge {name}')
    df = subnet2inc.merge(zip2inc, on='postal_code', how='left')
    df['network_key'] = hash_pandas_object(df['network'], index=False).values
    log.info(f'end merge {name},record_cnt={len(df)}')
    df['ip_type'] = name

    assert df.network_key.is_unique, f"problem merging {name}"
    assert df.network.is_unique, f"problem merging {name}"

    return df


def reorder_cols(df):
    to_cols = [
        'network_key',
        'ip_type',
        'network',
        'geoname_id',
        'postal_code',
        'latitude',
        'longitude',
        'country_iso_code',
        'country',
        'state_iso_code',
        'state',
        'city',
        'time_zone',
        'geo_id',
        'occup_housing_units',
        'median_household_income',
        'median_household_costs',
    ]

    mismatch = set(to_cols).symmetric_difference(df.columns)
    if mismatch:
        raise Exception(f"subnet2inc column mismatch,mismatch={mismatch}")

    df = df.reindex(columns=to_cols)
    return df


def main(config):
    cfg = config['demographics']
    log.info(f"start prepare zip demographics,config={cfg}")
    outdir = init_dir(cfg['demographics_d'], exist_ok=True, rmtree=True)

    ##################
    # start processing
    zip2inc = prep_zip2income(cfg)
    subnet2inc4, subnet2inc6 = prep_subnet2zip(cfg)

    # merge and write subnet2inc4

    df4 = merge_datasets(zip2inc, subnet2inc4, 'ip4')
    df6 = merge_datasets(zip2inc, subnet2inc6, 'ip6')

    df = pd.concat([df4, df6])
    df = reorder_cols(df)
    write_parquet(name='subnet2inc', df=df, outdir=outdir)

    log.info(f"end prepare zip demographics")
    return df
