from nbc_analysis.utils.file_utils import init_dir
from nbc_analysis.utils.log_utils import get_logger

from sqlalchemy import create_engine

from pathlib import Path

log = get_logger(__name__)


def init_db(cfg, metadata, replace=True):
    log.info(f'start initialize_database, cfg={cfg},replace={replace}')
    connect_str = cfg['connect_str']
    db_f = cfg.get('db_f')
    if db_f:
        connect_str = connect_str % db_f
        db_f = Path(db_f)
        init_dir(db_f.parent, exist_ok=True)

        if db_f.is_file():
            if not replace:
                raise Exception(f'database file already exists,db_file={db_f}')
            db_f.unlink()

    engine = create_engine(connect_str)
    metadata.create_all(engine)
    log.info(f'end initialize_database,connect_str={connect_str}')
    return engine


def get_db(cfg):
    log.info(f'start get db, cfg={cfg}')
    connect_str = cfg['connect_str']
    db_f = cfg.get('db_f')
    if db_f:
        connect_str = connect_str % db_f

    engine = create_engine(connect_str)
    log.info(f'end get_db,connect_str={connect_str}')
    return engine
