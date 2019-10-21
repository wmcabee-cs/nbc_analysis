from pandas.util import hash_pandas_object
from nbc_analysis.utils.debug_utils import retval


def build_hash_dim(data, hash_code, upd_ts, cols):
    dcols = [hash_code] + cols
    data[hash_code] = hash_pandas_object(data[cols], index=False).values
    dim = data[dcols].drop_duplicates().sort_values(cols)

    data.drop(columns=cols, inplace=True)
    return dim


def build_nk_dim(data, nk, pk, upd_ts, cols):
    dcols = [pk, nk] + cols + [upd_ts]
    f_drop_cols = [nk] + cols

    data[pk] = hash_pandas_object(data[[nk]], index=False).values

    df = data[dcols].drop_duplicates(pk)
    dim = df

    assert len(dim) == dim[nk].nunique(), "mismatch: %s != %s" % (len(dim), dim[nk].nunique())
    data.drop(columns=f_drop_cols, inplace=True)
    return dim


def build_unique_set(data, cols):
    values = data[cols].drop_duplicates().sort_values(cols)
    for field in cols:
        data[field] = data.pop(field)
    return values
