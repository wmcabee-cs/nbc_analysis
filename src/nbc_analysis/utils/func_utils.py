from toolz import take, first, cons, merge, partial

from nbc_analysis.utils.log_utils import get_logger

log = get_logger(__name__)


def take_if_limit(reader, limit, msg=""):
    if limit is not None:
        return take(limit, reader)
    return reader
