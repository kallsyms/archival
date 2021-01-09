#!/usr/bin/env python3
import glob
import itertools
import json
import logging
import pymongo
import sys


def grouper(n, iterable):
    """
    https://stackoverflow.com/a/8991553/8135152
    """
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('urllib3').setLevel(logging.INFO)

    mongo = pymongo.MongoClient("192.168.1.170", 27017)
    posts_collection = mongo.parler.posts

    for fn in glob.glob(sys.argv[1]):
        with open(fn, 'r') as f:
            print(fn)
            for batch in grouper(1000, f):

                jbatch = []
                for s in batch:
                    try:
                        jbatch.append(json.loads(s))
                    except json.decoder.JSONDecodeError as e:
                        print(f"Exception in {fn}: {e}")
                        continue

                try:
                    posts_collection.insert_many(jbatch, ordered=False)
                except pymongo.errors.BulkWriteError:
                    pass
                except UnicodeEncodeError:
                    for elem in jbatch:
                        try:
                            posts_collection.insert_one(elem)
                        except pymongo.errors.DuplicateKeyError:
                            pass
                        except UnicodeEncodeError:
                            pass
