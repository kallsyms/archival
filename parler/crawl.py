#!/usr/bin/env python3
import glob
import http.cookiejar
import json
import os
import requests
import time

cookies = http.cookiejar.MozillaCookieJar('cookies.txt')
cookies.load()

UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.129 Safari/537.36'

def crawl_while(url, params, jkey, cb):
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
                    if not cb(startkey, item):
                        return
                    yield item

                time.sleep(0.5)
            except Exception as e:
                print(f"{e}. Pausing for a bit...")
                time.sleep(30)


def crawl_all(url, params, jkey):
    yield from crawl_while(url, params, jkey, lambda _, __: True)


if __name__ == "__main__":
    try:
        os.mkdir("tags")
    except OSError:
        pass

    most_recent_post_for_tag = {}

    for tagf in glob.glob('tags/*.json'):
        tag = tagf.split('/')[1].split('.')[0]
        with open(tagf, 'r') as f:
            for line in f:
                d = json.loads(line)
                if tag not in most_recent_post_for_tag or int(d['createdAt']) > most_recent_post_for_tag[tag]:
                    most_recent_post_for_tag[tag] = int(d['createdAt'])

    print(f"Loaded existing tag crawl times: {most_recent_post_for_tag}")

    while True:
        for tag in crawl_all('https://api.parler.com/v1/hashtag', {'search': ''}, 'tags'):
            tag = tag['tag']
            print(f"Exploring #{tag}")
            with open(f'tags/{tag}.json', 'a') as f:
                if tag in most_recent_post_for_tag:
                    newposts = crawl_while('https://api.parler.com/v1/post/hashtag', {'tag': tag, 'limit': 10}, 'posts', lambda _, thing: int(thing['createdAt']) > most_recent_post_for_tag[tag])
                else:
                    newposts = crawl_all('https://api.parler.com/v1/post/hashtag', {'tag': tag, 'limit': 10}, 'posts')

                first_post = None
                for i, post in enumerate(newposts):
                    if i % 100 == 0:
                        print(f"{tag}:{post['createdAt']}")
                    if not first_post:
                        first_post = post
                    f.write(json.dumps(post) + '\n')

                if first_post:
                    most_recent_post_for_tag[tag] = int(first_post['createdAt'])

        time.sleep(60)
