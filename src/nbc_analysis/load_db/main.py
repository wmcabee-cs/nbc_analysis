from ..utils.debug_utils import retval
from .schema import initialize_db

from ..utils.file_utils import read_parquet
from ..utils.log_utils import get_logger
from toolz import merge
import numpy as np

log = get_logger(__name__)


def read_dims(indir, only=None):
    reader = (x.stem for x in sorted(indir.glob('dim_*.parquet')))
    if only is not None:
        reader = (x for x in reader if x in only)
    dims = {x: read_parquet(name=x, indir=indir) for x in reader}

    return dims


def _get_int64_cols(df):
    ds = (df.dtypes == 'uint64')
    cols = ds[ds].index.tolist()
    return cols


# TODO: sqlite could not handle unsigned 64 bit ints. Removing sign and casting to signed int
def _convert_uint64(df):
    cols = _get_int64_cols(df)
    for col in cols:
        df[col] = (df[col].values >> 1).astype(np.int64)
    return df


def apply_network_limit(network_dims, dim_network_limit):
    if dim_network_limit is not None:
        log.warn(f"applying dim_network_limit,limit={dim_network_limit}")
        if 'dim_network' in network_dims:
            dx = network_dims['dim_network']
            if dim_network_limit < len(dx):
                network_dims['dim_network'] = dx.iloc[:dim_network_limit].copy()


def load_dims(config, engine, only_tables=None, dim_network_limit=None):
    db_tables = set(engine.table_names())

    cal_dims = read_dims(config['calendar']['calendar_d'])
    normalized_dims = read_dims(config['normalize']['normalize_d'], only=only_tables)
    network_dims = read_dims(config['demographics']['demographics_d'], only=only_tables)
    apply_network_limit(network_dims, dim_network_limit)

    dims = merge(network_dims, cal_dims, normalized_dims)
    dims = {name: _convert_uint64(df) for name, df in dims.items()}
    expected_dims = {'dim_network', 'dim_video', 'dim_profile', 'dim_event_type', 'dim_end_type', 'dim_platform',
                     'dim_day', 'dim_day_loc', 'dim_hour_in_week'}

    names = sorted(dims)
    missing = set(names).symmetric_difference(expected_dims)
    if len(missing) != 0:
        if not only_tables:
            raise Exception(f"missing expected dims,missing={missing}")
        log.warn(
            f"some dimensions not loaded due to only_tables argument,only_tables={only_tables},missing={missing}")

    def _load_table(name):
        table_name = name.upper()
        df = dims[name]
        log.info(f'start load dim table {table_name},record_cnt={len(df)}')
        assert table_name in db_tables, f"table {table_name} not defined in database"
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False, method=None, chunksize=2000)

    reader = map(_load_table, names)
    for x in reader: pass


def load_fact(config, engine, limit, only_tables) -> None:
    indir = config['normalize']['normalize_d']

    name = 'f_video_end'

    if only_tables and name not in only_tables:
        log.warn("fact table not specified in only_tables, skipping")
        return

    table_name = name.upper()

    # TODO: Determine how to limit records from parquet file.
    #   - No chunk or file limit
    #   - Investigate using pyarrow directly
    #   - Also investigate memorymapping
    df = read_parquet(name, indir=indir)
    if limit and len(df) > limit:
        log.warn(f"limit for fact database load set.limit={limit}")
        df = df.iloc[:limit]

    log.info(f'start convert_uint64,name={name}')
    df = _convert_uint64(df)
    log.info(f'start load fact table {table_name},record_cnt={len(df)}')
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)


def main(config):
    log.info("start database load")
    cfg = config["database"]
    fact_limit = cfg.get('fact_limit')
    dim_network_limit = cfg.get('dim_network_limit')
    only_tables = cfg.get('only_tables')
    if only_tables:
        only_tables = set(only_tables)

    engine = initialize_db(cfg)
    load_dims(config, engine, only_tables=only_tables, dim_network_limit=dim_network_limit)
    load_fact(config, engine, limit=fact_limit, only_tables=only_tables)

    log.info("end database load")
    return engine
