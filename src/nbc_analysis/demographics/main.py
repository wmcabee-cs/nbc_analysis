from nbc_analysis.utils.log_utils import get_logger
from nbc_analysis.utils.file_utils import init_dir
from nbc_analysis.utils.debug_utils import retval

from .prep_zip2income.main import main as prep_zip2income
from .prep_subnet2zip.main import main as prep_subnet2zip

log = get_logger(__name__)


def main(config):
    cfg = config['demographics']
    log.info(f"start prepare zip demographics,config={cfg}")
    outdir = init_dir(cfg['demographics_d'], exist_ok=True, rmtree=True)
    outfile4 = outdir / cfg['subnet2inc4_filename']
    outfile6 = outdir / cfg['subnet2inc6_filename']

    zip2inc = prep_zip2income(cfg)
    subnet2inc4, subnet2inc6 = prep_subnet2zip(cfg)

    log.info(f'start merge and write subnet6inc')
    subnet2inc4 = subnet2inc4.merge(zip2inc, on='postal_code', how='left')
    subnet2inc4.to_parquet(outfile4, index=False)
    log.info(f'end merge and write subnet6inc,record_cnt={len(subnet2inc4)}')

    log.info('start merge and write subnet6inc')
    subnet2inc6 = subnet2inc6.merge(zip2inc, on='postal_code', how='left')
    subnet2inc6.to_parquet(outfile6, index=False)
    log.info(f'end merge and write subnet6inc,record_cnt={len(subnet2inc6)}')
    log.info(f"end prepare zip demographics")
