#!/usr/bin/env python3
import itertools
import logging
import pymongo
import sys

from parler import uuid_conv, get


"""
As of 2020-01-09, max IDs are ~
post: 389M
user: ???
comment: ???
"""

api_map = {
    'post': ('post', True),
    'user': ('profile', False),
    'comment': ('comment', True),
}

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    typ = sys.argv[1]
    start = 1
    if len(sys.argv) > 2:
        start = int(sys.argv[2])

    mongo = pymongo.MongoClient("192.168.1.170", 27017)
    collection = mongo.parler[typ]

    for i in itertools.count(start=start):
        if i % 100 == 0:
            logging.info("Progress: %d", i)

        uuid = uuid_conv(typ, i)
        if not uuid:
            continue

        # TODO: store seq id inline
        if collection.find_one({'_id': uuid}):
            continue

        thing = get(f'https://api.parler.com/v1/{api_map[typ][0]}', {'id': uuid})
        if thing is None:
            continue

        if api_map[typ][1] and typ not in thing:
            continue

        collection.replace_one({'_id': uuid}, (thing[typ] if api_map[typ][1] else thing), upsert=True)
