import logging
import http.cookiejar
import requests
import time


cookies = http.cookiejar.MozillaCookieJar('cookies.txt')
cookies.load()

UA = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36'

logger = logging.getLogger('parler')


def crawl(url, params, jkey):
    fail_count = 0
    startkey = 0
    last = False

    with requests.Session() as s:
        while not last:
            try:
                resp = s.get(url, params={**params, 'startkey': startkey}, headers={'User-Agent': UA}, cookies=cookies)
                if not resp.ok:
                    logger.info("Resp not ok: %s. Pausing for a bit...", resp)
                    fail_count += 1
                    if fail_count > 5:
                        logger.warning("Bailing from %s %s due to 5 errors in a row", url, params)
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

            except Exception:
                logger.exception("Exception fetching. Pausing for a bit...")
                time.sleep(30)
