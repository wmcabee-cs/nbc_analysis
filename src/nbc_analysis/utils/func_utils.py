from toolz import take, first, cons, merge, partial


def take_if_limit(reader, limit):
    if limit is not None:
        return take(limit, reader)
