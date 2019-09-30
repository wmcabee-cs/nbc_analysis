from toolz import take, first, cons, merge, partial


def take_if_limit(reader, limit):
    return take(limit, reader) if limit is not None else reader
