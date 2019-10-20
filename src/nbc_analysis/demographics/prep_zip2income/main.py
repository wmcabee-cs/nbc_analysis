from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.log_utils import get_logger

from toolz import first, concatv
import pandas as pd
import numpy as np

log = get_logger(__name__)


def load_acs_f(infile, limit=None):
    log.info(f"start load asc file,limit={limit},file={infile}")
    if limit:
        log.warn(f"limit set when reading asc file, {limit}")
        df = first(pd.read_csv(str(infile), iterator=True, chunksize=limit, engine='python', dtype=np.str))
    else:
        df = pd.read_csv(str(infile), dtype=np.str)
    df = df.set_index('GEO.id')
    log.info(f"end load asc file,file={infile},record_cnt={len(df)}")
    return df


def trim_columns(df):
    columns_est = [x for x in df.columns if '_EST_' in x]
    columns_oth = ['GEO.id2']
    cols = list(concatv(columns_oth, columns_est))
    dx = df[cols]
    return dx


def get_columns_to_keep(df):
    df = df.iloc[0].to_frame('label')
    df.index.name = 'var_name'
    df = df.reset_index().set_index('label')
    keep_columns = [
        'Id2',
        'Occupied housing units; Estimate; Occupied housing units',
        'Occupied housing units; Estimate; HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2017 INFLATION-ADJUSTED DOLLARS) - Median household income (dollars)',
        'Occupied housing units; Estimate; MONTHLY HOUSING COSTS - Median (dollars)',
    ]
    keep_ids = df.loc[keep_columns].var_name.tolist()
    return keep_ids


def clean_fields(df, cols):
    measures = df[cols]
    mask = measures.apply(lambda x: x.str.isdecimal())
    measures = measures.where(mask)
    df[cols] = measures.astype(np.float)
    return df


def main(cfg):
    log.info(f"start prep_zip2income")
    infile = cfg['zip2income_input_f']
    limit = cfg.get('record_limit')

    df = load_acs_f(infile, limit=limit)
    df = trim_columns(df)
    keep_ids = get_columns_to_keep(df)
    df = df[keep_ids][1:].reset_index()
    df.columns = ['geo_id', 'postal_code', 'occup_housing_units', 'median_household_income', 'median_household_costs']
    df = df.drop_duplicates(subset=['postal_code'])
    df = clean_fields(df, cols=['occup_housing_units', 'median_household_income', 'median_household_costs'])
    log.info(f"end prep_zip2income,record_cnt={len(df)}")
    return df
