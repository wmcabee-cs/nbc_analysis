import pytricia
from nbc_analysis.utils.toml_utils import get_config
from nbc_analysis.utils.file_utils import read_parquet
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.func_utils import take_if_limit
from nbc_analysis.utils.log_utils import get_logger
from pathlib import Path

from toolz import *

log = get_logger(__name__)


def load_ips(normalize_cfg):
    indir = Path(normalize_cfg['normalize_d'])
    ips = read_parquet('ips', indir=indir)
    return ips


def load_network_lkup(demographics_cfg):
    indir = Path(demographics_cfg['demographics_d'])
    network_lkup = read_parquet('network_dim.parquet', indir=indir, columns=['network', 'network_key', 'time_zone'])
    return network_lkup


def build_ptree(network, limit=None):
    ptree = pytricia.PyTricia(128)

    def add_network(rec):
        ptree.insert(rec.network, rec.network_key)

    log.info(f'start build ip ptree,record_cnt={len(network)},limit={limit}')
    reader = network.itertuples()
    reader = take_if_limit(reader=reader, limit=limit, msg="limit set subnet read")
    reader = (x for x in reader if x.network_key != 0)
    reader = map(add_network, reader)
    for x in reader: pass
    log.info('end build ip ptree')

    return ptree


def get_network_keys(ptree, ips, limit=None):
    def get_network_key(ip):
        return ptree.get(ip, 0)

    log.info(f"start get network keys,record_cnt={len(ips)},limit={limit}")
    df = ips.sample(limit) if limit and limit < len(ips) else ips
    df['network_key'] = df.ip.map(get_network_key)

    N = len(df)
    hits = (df.network_key != 0).sum()
    hit_rate = hits / N
    log.info(f"end get network keys,N={N},hits={hits},hit_rate={hit_rate:.2f},limit={limit}")
    return df


def main(config, ips):
    demographics_cfg = config['demographics']
    network_lkup = load_network_lkup(demographics_cfg)
    ptree = build_ptree(network_lkup)
    df = get_network_keys(ips=ips, ptree=ptree)
    return df
