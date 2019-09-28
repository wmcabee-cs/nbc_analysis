import arrow
import re


def parse_day(day):
    m = re.match(r"(?P<year>\d{4})(?P<month>\d{2})(?P<day_num>\d{2})", day)
    if not m:
        raise ValueError(f"Invalid date format '{day}'")

    return m.groupdict()


def get_now_zulu():
    return arrow.utcnow().isoformat('T').replace('+00:00', 'Z')
