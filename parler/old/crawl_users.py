#!/usr/bin/env python3
import itertools
import logging
import pymongo
import string
import sys

from parler import crawl


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    mongo = pymongo.MongoClient("192.168.1.170", 27017)
    users_collection = mongo.parler.users

    start = 'aaa'
    if len(sys.argv) > 1:
        start = sys.argv[1]

    for i, chars in enumerate(itertools.product(*([string.ascii_lowercase]*3))):
        prefix = ''.join(chars)
        if prefix < start:
            continue

        logging.debug(prefix)

        if i % 100 == 0:
            logging.info(prefix)

        for user in crawl('https://api.parler.com/v1/users', {'search': prefix}, 'users'):
            user['_id'] = user['id']
            users_collection.replace_one({'_id': user['_id']}, user, upsert=True)
