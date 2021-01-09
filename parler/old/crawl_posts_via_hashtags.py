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
    tags_collection = mongo.parler.tags
    posts_collection = mongo.parler.posts

    NOT_BEFORE = 20210101000000

    while True:
        for tag in get_tags():
            most_recent = tag.get('mostRecentPost', NOT_BEFORE)
            tag = tag['tag']

            logging.info("Exploring %s", tag)
            posts = crawl('https://api.parler.com/v1/post/hashtag', {'tag': tag, 'limit': 10}, 'posts')

            first_post = None
            for i, post in enumerate(posts):
                if int(post['createdAt']) <= most_recent:
                    break

                if i % 100 == 0:
                    logging.info("%s:%s", tag, post['createdAt'])

                if not first_post:
                    first_post = post

                post['_id'] = post['id']
                posts_collection.replace_one({'_id': post['_id']}, post, upsert=True)

            if first_post:
                ctime = int(first_post['createdAt'])
                tags_collection.update_one({'tag': tag}, {'$set': {'mostRecentPost': ctime}}, upsert=True)

        time.sleep(60)
