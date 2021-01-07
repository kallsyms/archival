#!/usr/bin/env python3
import collections
import logging
import pymongo
import string
import time

from parler import crawl


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    mongo = pymongo.MongoClient("192.168.1.170", 27017)
    posts_collection = mongo.parler.posts
    comments_collection = mongo.parler.comments

    while True:
        cur = posts_collection.find(
            {
                'comments': {'$ne': "0"},
                'parent': {'$exists': False},  # don't care about retweets, just the original
            },
            sort=[('comments', pymongo.DESCENDING)],
            no_cursor_timeout=True,
        )

        for post in cur:
            pid = post['_id']
            most_recent_comment_time = 0
            #c = comments_collection.find_one({'post': pid}, sort=[('createdAt', pymongo.DESCENDING)])
            c = comments_collection.find_one({'post': pid})
            if c:
                continue

            # if c:
            #     most_recent_comment_time = int(c['createdAt'])
            #     logging.debug("Got existing most recent timestamp %d", most_recent_comment_time)

            logging.info("Getting comments for %s", pid)
            comments = crawl('https://api.parler.com/v1/comment', {'id': pid}, 'comments')

            count = 0
            for comment in comments:
                if int(post['createdAt']) <= most_recent_comment_time:
                    break

                comments_collection.replace_one({'_id': comment['_id']}, comment, upsert=True)
                count += 1

            logging.info("Got %d comments for %s", count, pid)

        cur.close()
        time.sleep(300)
