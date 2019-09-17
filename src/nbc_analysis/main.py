# -*- coding: utf-8 -*-
import json
from toolz import first, concat, assoc
from itertools import starmap
import pandas as pd
import pprint
from .utils.debug_utils import retval

HEADER = {
    'events',
    'mpid',
    'timestamp_unixtime_ms',
    'batch_id',
    'message_id',
    'source_request_id',
    'message_type',
    'schema_version',
    'application_info',
    'source_info',
    'ip',
    'consent_state',
    'attributes',
    'filename',
    'user_identities',
    'attribution_info',
}


def parse_file(file):
    text = file.read_text()
    name = file.name

    events = text.replace("\n}\n{\n", "}<split>{")
    events = events.split("<split>")

    reader = iter(events)
    reader = map(json.loads, reader)
    reader = (assoc(x, 'filename', name) for x in reader)
    return reader


def read_files(indir):
    reader = indir.glob('*.txt')

    return concat(map(parse_file, reader))


def check_for_nested(reader):
    hits = {}

    def func(event):

        for key, value in event.items():
            if type(value) in {dict, list} and key not in hits:
                hits[key] = (event['filename'], event['row_idx'], value)
        return event

    reader = map(func, reader)
    for x in reader:
        yield x

    if len(hits) > 0:
        pprint.pprint(hits)
        raise Exception("Nested fields:", hits)


def flatten_dict(event, field):
    if field not in event:
        return event

    for key, value in event.pop(field).items():
        event[f"{field}_{key}"] = value
    return event


def fix_apple_search_ads_attributes(event):
    field = 'application_info_apple_search_ads_attributes'

    if field not in event:
        return None

    version_field = f"{field}_ver"
    attrs_count_field = f"{field}_attrs"

    value = event.pop(field)
    assert len(value) == 1
    version, attrs = first(value.items())
    event[version_field] = version
    if len(attrs) == 1:
        if attrs.get('iad-attribution', 'missing') == 'false':
            event[attrs_count_field] = 0
            return None
    event[attrs_count_field] = len(attrs)
    attrs['row_idx'] = event['row_idx']
    return attrs


def fix_user_identities(event):
    field = 'user_identities'
    if field not in event:
        return None

    identities = event.pop(field)
    identities = (assoc(x, 'row_idx', event['row_idx']) for x in identities)
    return identities


def flatten_event_detail(event):
    #################################
    # Add event detail (from events field) to record
    #################################

    event_detail = event.pop('events')

    # Making assumptions about data type.  Add guard to catch exceptions
    # TODO: Confirm will always have a single event in events list
    assert len(event_detail) == 1
    event_detail = first(event_detail)
    keys = set(event_detail)
    assert keys == {'data', 'event_type'}

    data = event_detail['data']
    data['event_type'] = event_detail['event_type']
    custom_attributes = data.pop('custom_attributes')

    # make sure not overwriting existing fields
    # TODO: Confirm all data in event detail is already in the parent record
    for key, value in data.items():
        event[f"detail_{key}"] = value

    #################################
    # Add custom attributes to record
    #################################
    if not len(custom_attributes) > 0:
        event['has_custom_attributes'] = 0
        return None

    event['has_custom_attributes'] = 1

    to_fields = set(event)
    from_fields = set(custom_attributes)
    fields_exist = to_fields.intersection(from_fields)
    if len(fields_exist) > 0:
        raise Exception(f"fields exist in both custom and event fields, {event}")

    for key, value in custom_attributes.items():
        event[key] = value
    return None


def flatten_event(row_idx, event):
    # record input records
    header = set(event.keys())

    # flatten nested structure into set of dataframes
    event['row_idx'] = row_idx
    flatten_event_detail(event)
    flatten_dict(event, 'application_info')
    flatten_dict(event, 'attributes')
    flatten_dict(event, 'consent_state')
    flatten_dict(event, 'source_info')
    flatten_dict(event, 'attribution_info')
    apple_add_attrs = fix_apple_search_ads_attributes(event)
    user_identities = fix_user_identities(event)

    # event['application_info'] =
    missing = header - HEADER
    if len(missing):
        raise Exception(f"new header fields in file {event['filename']}: {missing}")

    return event, apple_add_attrs, user_identities


def write_events(events, outdir):
    reader = check_for_nested(events)
    df = pd.DataFrame.from_records(reader)

    def clean_columns(x):
        x = x.lower().replace(' ', '_')
        x = x.replace(':', '_')
        x = x.replace('%', 'pct')
        return x

    df.columns = df.columns.map(clean_columns)
    outfile = outdir / 'events.csv'
    df.to_csv(outfile, index=False)
    print(f">> wrote {outfile},rows={len(df)}")
    return df


def write_idents(idents, outdir):
    idents = filter(None, idents)
    idents = concat(idents)
    df = pd.DataFrame.from_records(idents)

    outfile = outdir / 'user_identies.csv'
    df.to_csv(outfile, index=False)
    print(f">> wrote {outfile},rows={len(df)}")
    return df


def write_appl_attrs(appl_attrs, outdir):
    appl_attrs = filter(None, appl_attrs)

    df = pd.DataFrame.from_records(appl_attrs)

    def clean_columns(x):
        return x.replace('-', '_')

    df.columns = df.columns.map(clean_columns)

    outfile = outdir / 'apple_add_attrs.csv'
    df.to_csv(outfile, index=False)
    print(f">> wrote {outfile},rows={len(df)}")
    return df


def write_custom_attrs(custom_attrs, outdir):
    custom_attrs = filter(None, custom_attrs)

    def clean_columns(x):
        return x.replace(' ', '_')

    df = pd.DataFrame.from_records(custom_attrs)
    df.columns = df.columns.map(clean_columns)
    outfile = outdir / 'custom_attrs.csv'
    df.to_csv(outfile, index=False)
    return df


def main(indir, outdir):
    reader = read_files(indir)
    reader = enumerate(reader)
    reader = starmap(flatten_event, reader)
    events, appl_attrs, idents = zip(*reader)
    write_appl_attrs(appl_attrs, outdir)
    write_idents(idents, outdir)
    df = write_events(events, outdir)
    # df = write_custom_attrs(custom_attrs, outdir)
    print('ok')
    return df
