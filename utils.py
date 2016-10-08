import logging
from time import mktime, struct_time
from typing import Any, Dict, Optional

from google.protobuf.timestamp_pb2 import Timestamp

from pb_gen.feed_pb2 import Entry, Feed

logger = logging.getLogger(__name__)


def _toTimestamp(date: struct_time = None) -> Optional[Timestamp]:
    if date is None:
        return None
    timestamp = mktime(date)
    secs = int(timestamp)
    nanos = int((timestamp - secs) * 10**9)
    pb_timestamp = Timestamp(seconds=secs, nanos=nanos)
    return pb_timestamp


def create_entry(raw_data: Dict[str, Any]) -> Entry:
    entry_kwargs = {
        'link': raw_data.get('link'),
        'summary': raw_data.get('summary'),
        'published': _toTimestamp(raw_data.get('published_parsed')),
        'author': raw_data.get('author'),
        'media_content': str(raw_data.get('media_content', {})),
    }

    entry = Entry(**entry_kwargs)
    return entry


def create_feed(raw_data: Dict[str, Any]) -> Feed:
    feed_kwargs = {
        'link': raw_data.get('link'),
        'title': raw_data.get('title'),
        'updated': _toTimestamp(raw_data.get('updated_parsed')),
        'entries': [],
    }

    feed = Feed(**feed_kwargs)
    return feed
