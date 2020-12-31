#!/usr/bin/env python3
import collections
import logging
import pymongo
import string
import time

from parler import crawl


def get_tags():
    # Old way of enumerating through /v1/hashtag with empty search doesn't work any more.
    # Go through char by char
    seen = set()
    for char in string.ascii_lowercase:
        logging.debug("Tag iter '%s'", char)
        for tag in crawl('https://api.parler.com/v1/hashtag', {'search': char}, 'tags'):
            if 'k' not in tag['totalPosts']:  # count < 1000, don't care for now
                break
            if tag['tag'] not in seen:
                yield tag
                seen.add(tag['tag'])


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    mongo = pymongo.MongoClient("192.168.1.170", 27017)
    posts_collection = mongo.parler.posts
    comments_collection = mongo.parler.comments

    while True:
        for post in posts_collection.find({'comments': {'$ne': "0"}}):
            pid = post['_id']
            most_recent_comment_time = 0
            c = comments_collection.find_one({'post': pid}, sort=[('createdAt', pymongo.DESCENDING)])
            if c:
                most_recent_comment_time = int(c['createdAt'])
                logging.debug("Got existing most recent timestamp %d", most_recent_comment_time)

            logging.info("Getting comments for %s", pid)
            comments = crawl('https://api.parler.com/v1/comment', {'id': pid}, 'comments')

            for comment in comments:
                if int(post['createdAt']) <= most_recent_comment_time:
                    break

                comments_collection.update({'_id': comment['_id']}, comment, upsert=True)

        time.sleep(300)
