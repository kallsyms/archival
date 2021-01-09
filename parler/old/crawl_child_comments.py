#!/usr/bin/env python3
import collections
import datetime
import logging
import pymongo
import string
import time

from parler import crawl


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    mongo = pymongo.MongoClient("192.168.1.170", 27017)
    comments_collection = mongo.parler.comments

    while True:
        cur = comments_collection.find(
            {
                'comments': {'$ne': "0"},
                '_descendants_crawled': {'$exists': False},
            },
            sort=[('comments', pymongo.DESCENDING)],
            no_cursor_timeout=True,
        )

        for root_comment in cur:
            cid = root_comment['_id']

            logging.info("Getting child comments for %s", cid)
            comments = crawl('https://api.parler.com/v1/comment', {'id': cid}, 'comments')

            count = 0
            for comment in comments:
                comments_collection.replace_one({'_id': comment['_id']}, comment, upsert=True)
                count += 1

            comments_collection.update_one({'_id': cid}, {'$set': {'_descendants_crawled': datetime.datetime.utcnow()}})

            logging.info("Got %d child comments for %s", count, cid)

        cur.close()
        time.sleep(300)
