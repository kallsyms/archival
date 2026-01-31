import pycurl
import certifi
from io import BytesIO

def get(path: str) -> bytes:
    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, 'https://truthsocial.com' + path)
    c.setopt(c.COOKIEJAR, 'cookies')
    c.setopt(c.HTTPHEADER, [
        'accept: application/json, text/plain, */*',
        'accept-language: en-US,en;q=0.9',
        'authorization: Bearer CO6AVNdBwmqD1nFrF4TU1RiV1Hhp6c-IET7VTZlosQg',
        'dnt: 1',
        'priority: u=1, i',
        'referer: https://truthsocial.com/',
        'sec-ch-ua: "Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile: ?0',
        'sec-ch-ua-platform: "macOS"',
        'sec-fetch-dest: empty',
        'sec-fetch-mode: cors',
        'sec-fetch-site: same-origin',
        'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    ])
    c.setopt(c.WRITEDATA, buffer)
    c.setopt(c.CAINFO, certifi.where())
    c.perform()
    c.close()

    return buffer.getvalue()

def get_stream(path: str, cb):
    c = pycurl.Curl()
    c.setopt(c.URL, 'https://truthsocial.com' + path)
    c.setopt(c.COOKIEJAR, 'cookies')
    c.setopt(c.HTTPHEADER, [
        'accept: application/json, text/plain, */*',
        'accept-language: en-US,en;q=0.9',
        'authorization: Bearer CO6AVNdBwmqD1nFrF4TU1RiV1Hhp6c-IET7VTZlosQg',
        'dnt: 1',
        'priority: u=1, i',
        'referer: https://truthsocial.com/',
        'sec-ch-ua: "Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-mobile: ?0',
        'sec-ch-ua-platform: "macOS"',
        'sec-fetch-dest: empty',
        'sec-fetch-mode: cors',
        'sec-fetch-site: same-origin',
        'user-agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    ])
    c.setopt(c.WRITEFUNCTION, cb)
    c.setopt(c.CAINFO, certifi.where())
    c.perform()
    c.close()

#print(get('/api/v1/streaming/user').decode('utf-8'))
get_stream('/api/v1/streaming/user', lambda data: print(data.decode('utf-8')))
