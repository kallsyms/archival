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
    users_collection = mongo.parler.users
    posts_collection = mongo.parler.posts

    while True:
        cur = users_collection.find({
            'score': {'$ne': "0"},
            '_post_crawl_time': {'$exists': False},
        }, no_cursor_timeout=True)
        for user in cur:
            uid = user['_id']
            most_recent = user.get('_post_crawl_time', 0)

            logging.info("Exploring %s", uid)
            posts = crawl('https://api.parler.com/v1/post/creator', {'id': uid, 'limit': 20}, 'posts')

            for post in posts:
                if int(post['createdAt']) <= most_recent:
                    break

                posts_collection.replace_one({'_id': post['_id']}, post, upsert=True)

            users_collection.update_one({'_id': uid}, {'$set': {'_post_crawl_time': datetime.datetime.utcnow()}})

        cur.close()

        time.sleep(60)
