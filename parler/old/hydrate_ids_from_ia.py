#!/usr/bin/env python3
import collections
import datetime
import logging
import pymongo
import re
import requests
import string
import time

from parler import get

post_regex = re.compile(b'https://parler.com/post/([0-9a-f]{32})')

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    mongo = pymongo.MongoClient("192.168.1.170", 27017)
    posts_collection = mongo.parler.posts

    cdx = requests.get('https://web.archive.org/cdx/search/cdx?url=parler.com/post/&matchType=prefix', stream=True)

    for cdx_line in cdx.iter_lines():
        _, _, url, _, _, _, _ = cdx_line.split(b' ')
        match = post_regex.match(url)
        if not match:
            continue

        pid = match.group(1).decode('ascii')

        if posts_collection.find_one({'_id': pid}):
            continue

        logging.info("Hydrating %s", pid)

        post = get('https://api.parler.com/v1/post', {'id': pid})
        if post is None:
            continue

        posts_collection.replace_one({'_id': pid}, post['post'], upsert=True)


    time.sleep(60)


