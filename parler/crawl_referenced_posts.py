#!/usr/bin/env python3
import collections
import datetime
import logging
import pymongo
import string
import time

from parler import get


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    mongo = pymongo.MongoClient("192.168.1.170", 27017)
    posts_collection = mongo.parler.posts

    while True:
        cur = posts_collection.find(
            {
                'parent': {'$exists': True},
            },
            no_cursor_timeout=True,
        )

        for post in cur:
            parent_id = post['parent']

            if posts_collection.find_one({'_id': parent_id}):
                continue

            logging.info("Getting parent post of %s", post['_id'])
            post = get('https://api.parler.com/v1/post', {'id': parent_id})
            if post is None:
                continue

            posts_collection.replace_one({'_id': parent_id}, post['post'], upsert=True)

        cur.close()

        time.sleep(60)
