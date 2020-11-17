#!/usr/bin/env python3
import collections
import glob
import http.cookiejar
import json
import os
import requests
import string
import time

cookies = http.cookiejar.MozillaCookieJar('cookies.txt')
cookies.load()

UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'

def crawl(url, params, jkey):
    fail_count = 0
    startkey = 0
    last = False

    with requests.Session() as s:
        while not last:
            try:
                resp = s.get(url, params={**params, 'startkey': startkey}, headers={'User-Agent': UA}, cookies=cookies)
                if not resp.ok:
                    print(f"Resp not ok: {resp}. Pausing for a bit...")
                    fail_count += 1
                    if fail_count > 5:
                        print(f"Bailing from {url} {params} due to 5 errors in a row")
                        return

                    time.sleep(10)
                    continue

                fail_count = 0

                j = resp.json()
                startkey = j['next']
                last = j['last']

                for item in j[jkey]:
                    yield item

                time.sleep(0.5)
            except Exception as e:
                print(f"{e}. Pausing for a bit...")
                time.sleep(30)


def get_tags():
    # Old way of enumerating through /v1/hashtag with empty search doesn't work any more.
    # Go through char by char
    seen = set()
    for char in string.ascii_lowercase:
        print(f"Tag iter '{char}'")
        for tag in crawl('https://api.parler.com/v1/hashtag', {'search': char}, 'tags'):
            if 'k' not in tag['totalPosts']:  # count < 1000
                break
            if tag['tag'] not in seen:
                yield tag
                seen.add(tag['tag'])


if __name__ == "__main__":
    try:
        os.mkdir("tags")
    except OSError:
        pass

    NOT_BEFORE = 20201101000000
    most_recent_post_for_tag = collections.defaultdict(lambda: NOT_BEFORE)

    for tagf in glob.glob('tags/*.json'):
        tag = tagf.split('/')[1].split('.')[0]
        with open(tagf, 'r') as f:
            for line in f:
                d = json.loads(line)
                if tag not in most_recent_post_for_tag or int(d['createdAt']) > most_recent_post_for_tag[tag]:
                    most_recent_post_for_tag[tag] = int(d['createdAt'])

    print(f"Loaded existing tag crawl times: {most_recent_post_for_tag}")

    while True:
        for tag in get_tags():
            tag = tag['tag']
            print(f"Exploring #{tag}")
            safetag = tag.replace('.','').replace('/','')
            with open(f'tags/{safetag}.json', 'a') as f:
                newposts_iter = crawl('https://api.parler.com/v1/post/hashtag', {'tag': tag, 'limit': 10}, 'posts')

                first_post = None
                for i, post in enumerate(newposts_iter):
                    if int(post['createdAt']) <= most_recent_post_for_tag[safetag]:
                        break

                    if i % 100 == 0:
                        print(f"{tag}:{post['createdAt']}")
                    if not first_post:
                        first_post = post
                    f.write(json.dumps(post) + '\n')

                if first_post:
                    most_recent_post_for_tag[safetag] = int(first_post['createdAt'])

        time.sleep(60)
