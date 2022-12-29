from typing import Optional
import logging
import http.cookiejar
import requests
import time


cookies = http.cookiejar.MozillaCookieJar('cookies.txt')
cookies.load()

UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'

logger = logging.getLogger('parler')


def uuid_conv(typ: str, iid: int) -> Optional[str]:
    """
    https://github.com/d0nk/parler-tricks/blob/main/parler/conversion.py#L22
    """
    for i in range(5):
        try:
            resp = requests.get(f'https://api.parler.com/v3/uuidConversion/{typ}/{iid}', headers={'User-Agent': UA}, cookies=cookies)
            if not resp.ok:
                if resp.status_code == 404:
                    logger.info("Bailing from %s %d - 404", typ, iid)
                    return None

                logger.info("Resp not ok: %s. Pausing for a bit...", resp)
                time.sleep(3**i)
                continue

            return resp.text.strip()

        except Exception:
            logger.exception("Exception fetching. Pausing for a bit...")
            time.sleep(30)

    logger.warning("Bailing from uuid_conv of %s %d due to 5 errors in a row", typ, iid)
    return None


def get(url, params):
    for i in range(5):
        try:
            resp = requests.get(url, params=params, headers={'User-Agent': UA}, cookies=cookies)
            if not resp.ok:
                # parler uses 400 to denote basically a 403
                if resp.status_code == 400:
                    logger.info("Bailing from %s %s - got a 400", url, params)
                    return
                if resp.status_code == 404:
                    logger.info("Bailing from %s %s - 404", url, params)
                    return
                elif resp.status_code == 429 and 'x-ratelimit-reset' in resp.headers:
                    logger.debug("429 on %s %s. Waiting until ratelimit reset", url, params)
                    time.sleep(int(resp.headers['x-ratelimit-reset']) - int(time.time()) + 2)
                    continue
                else:
                    logger.info("Resp not ok: %s. Pausing for a bit...", resp)
                    time.sleep(3**i)
                    continue

            return resp.json()

        except Exception:
            logger.exception("Exception fetching. Pausing for a bit...")
            time.sleep(30)

    logger.warning("Bailing from %s %s due to 5 errors in a row", url, params)
    return


def crawl(url, params, jkey):
    fail_count = 0
    startkey = 0
    last = False

    with requests.Session() as s:
        while not last:
            try:
                resp = s.get(url, params={**params, 'startkey': startkey}, headers={'User-Agent': UA}, cookies=cookies)
                if not resp.ok:
                    # parler uses 400 to denote basically a 403
                    if resp.status_code == 400:
                        logger.info("Bailing from %s %s - got a 400", url, params)
                        return
                    elif resp.status_code == 429 and 'x-ratelimit-reset' in resp.headers:
                        logger.debug("429 on %s %s. Waiting until ratelimit reset", url, params)
                        time.sleep(int(resp.headers['x-ratelimit-reset']) - int(time.time()) + 2)
                        continue
                    else:
                        logger.info("Resp not ok: %s. Pausing for a bit...", resp)
                        fail_count += 1
                        if fail_count > 5:
                            logger.warning("Bailing from %s %s due to 5 errors in a row", url, params)
                            return

                        time.sleep(3**fail_count)
                        continue

                fail_count = 0

                j = resp.json()
                startkey = j['next']
                last = j['last']

                for item in j[jkey]:
                    yield item

                time.sleep(0.5)

            except Exception:
                logger.exception("Exception fetching. Pausing for a bit...")
                time.sleep(30)
