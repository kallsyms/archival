#!/usr/bin/env python3
import boto3
import logging
import os
import queue
import requests
import threading
import time
import zstandard as zstd

stream_url = "https://api.twitter.com/2/tweets/sample/stream?expansions=author_id,geo.place_id&place.fields=country,full_name,geo,id,name&tweet.fields=author_id,created_at,geo,id,possibly_sensitive,withheld&user.fields=id,name,protected,username,verified"

S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ['S3_PREFIX']
S3_STORAGE_CLASS = os.environ['S3_STORAGE_CLASS']
S3_REGION = os.environ['S3_REGION']
S3_ENDPOINT = os.environ['S3_ENDPOINT']
S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
S3_SECRET_KEY = os.environ['S3_SECRET_KEY']

TWITTER_TOKEN = os.environ['TWITTER_TOKEN']
FILE_CHUNK_SIZE = int(os.environ['FILE_CHUNK_SIZE'])

def batcher(q):
    while True:
        fn = time.strftime('twitter_%Y%m%d%H%M%S.json.zstd')
        logging.info("Batch saving to file %s", fn)

        try:
            with open(fn, 'wb') as fh:
                cctx = zstd.ZstdCompressor()
                with cctx.stream_writer(fh) as compressor:
                    while fh.tell() < FILE_CHUNK_SIZE:
                        line = q.get()
                        compressor.write(line + b'\n')

            logging.info("Uploading %s to S3", fn)
            s3_client = boto3.client('s3', region_name=S3_REGION, endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)
            s3_client.upload_file(fn, S3_BUCKET, os.path.join(S3_PREFIX, fn), ExtraArgs={'StorageClass': S3_STORAGE_CLASS})
            os.remove(fn)
        except KeyboardInterrupt:
            raise
        except Exception:
            logging.exception("Exception batching")
            os.remove(fn)


def twitter_stream(q):
    while True:
        logging.info("Starting stream from twitter")
        try:
            response = requests.get(
                stream_url,
                headers={
                    'Authorization': f'Bearer {TWITTER_TOKEN}',
                },
                stream=True
            )
            if response.status_code == 429:
                logging.warning("Received 429. Backing off for 10 mins")
                time.sleep(600)
                continue

            for response_line in response.iter_lines():
                q.put(response_line)

            # Connection closed gracefully, reset after a second
            time.sleep(1)
        except KeyboardInterrupt:
            raise
        except Exception:
            logging.exception("Exception from twitter stream")
            time.sleep(5)
            continue


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    q = queue.Queue()
    threads = [
        threading.Thread(target=twitter_stream, args=(q, )),
        threading.Thread(target=batcher, args=(q, )),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
