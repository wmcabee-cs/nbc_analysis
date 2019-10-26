from typing import Iterable
import pandas as pd
from pandas.util import hash_pandas_object
from nbc_analysis.utils.debug_utils import retval
import numpy as np


def mk_hash(df, cols):
    return (hash_pandas_object(df[cols], index=False).values >> 1).astype(np.int64)


def build_hash_dim(data: pd.DataFrame,
                   hash_code: str,
                   upd_ts: str,
                   cols: Iterable[str]) -> pd.DataFrame:
    dcols = [hash_code] + cols + [upd_ts]
    data[hash_code] = mk_hash(data, cols)
    dim = data[dcols].drop_duplicates(hash_code).sort_values(cols)

    data.drop(columns=cols, inplace=True)
    return dim


def build_nk_dim(data: pd.DataFrame,
                 nk: str,
                 pk: str,
                 upd_ts: str,
                 cols: Iterable[str]) -> pd.DataFrame:
    dcols = [pk, nk] + cols + [upd_ts]
    f_drop_cols = [nk] + cols

    data[pk] = mk_hash(data, [nk])

    df = data[dcols].drop_duplicates(pk)
    dim = df

    assert len(dim) == dim[nk].nunique(), "mismatch: %s != %s" % (len(dim), dim[nk].nunique())
    data.drop(columns=f_drop_cols, inplace=True)
    return dim


def build_unique_set(data: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    values = data[cols].drop_duplicates().sort_values(cols)
    for field in cols:
        data[field] = data.pop(field)
    return values
