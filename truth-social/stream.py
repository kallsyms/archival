import mastodon
import logging
# import urllib3.util
import httpx

logging.basicConfig(level=logging.DEBUG)

# import http.client as http_client
# http_client.HTTPConnection.debuglevel = 1
#
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

api_req = mastodon.internals.Mastodon._Mastodon__api_request
def req(*args, **kwargs):
    headers = kwargs.get('headers', {})
    headers['accept'] = 'application/json, text/plain, */*'
    headers['accept-language'] = 'en-US,en;q=0.9'
    #headers['Accept-Encoding'] = urllib3.util.SKIP_HEADER
    headers['authorization'] = 'Bearer CO6AVNdBwmqD1nFrF4TU1RiV1Hhp6c-IET7VTZlosQg'
    headers['dnt'] = '1'
    headers['priority'] = 'u=1, i'
    headers['referer'] = 'https://truthsocial.com/'
    headers['sec-ch-ua'] = '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"'
    headers['sec-ch-ua-mobile'] = '?0'
    headers['sec-ch-ua-platform'] = '"macOS"'
    headers['sec-fetch-dest'] = 'empty'
    headers['sec-fetch-mode'] = 'cors'
    headers['sec-fetch-site'] = 'same-origin'
    headers['user-agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
    kwargs['headers'] = headers
    return api_req(*args, **kwargs)

_set_cookie_header = httpx.Cookies.set_cookie_header
def set_cookie_header(self, request):
    print(self)
    print(request)
    _set_cookie_header(self, request)
    request.headers._list.sort(key=lambda x: x[1])
    print(request.headers)

httpx.Cookies.set_cookie_header = set_cookie_header

mastodon.internals.Mastodon._Mastodon__api_request = req

client = mastodon.Mastodon(access_token=None, user_agent=None, api_base_url='https://truthsocial.com')
client.session = httpx.Client(http2=True)
del client.session._headers['accept-encoding']

client.session.cookies.set("__cf_bm", "vtXJ6b8q50278h8WngYWFSx.rZed5eQOPwnnLuc0hFA-1730659567-1.0.1.1-uPr95hfcpbY2PBKFEStwreuiYDRVi8mJNZzDDwbP6CEJYe2jC162hRd4x1ETLCKPTAduyOEAH4l9xFhJyz.YtA", domain="truthsocial.com")
client.session.cookies.set("_cfuvid", "U38yamLw6UXJQ7pqxIaZkIhmaEX.lOjr0H0fDpW47vI-1730659567128-0.0.1.1-604800000", domain="truthsocial.com")
client.session.cookies.set("__cflb", "0H28vTPqhjwKvpvovPJSCEhDcnGBa5JYeGpH5iMVzhV", domain="truthsocial.com")
client.session.cookies.set("_tq_id.TV-5427368145-1.4081", "1f861493db98a9fe.1730659568.0.1730659568..", domain="truthsocial.com")
client.session.cookies.set("_fbp", "fb.1.1730659567759.344237573766365517", domain="truthsocial.com")
client.session.cookies.set("_mastodon_session", "YYIAKKuMVMhNQcsNQzkR0FD0aMBXDQ5HFwJx%2BycXDIEIfyHeZe4mbtdXPqwO66O8I4iK4tpXbVHs%2B1JEzZhFIo%2B2vFm22mxDL3qbxDCBqebnjJ4lPYw%2Fq3grYTCZLtHP9paTZh5%2BWXq4NrDdslJYDUGUNyCbQAAO5IU78wqlq6%2FfSdB8AfN9Lli3yfRzzsnyFOfHNT2xp5%2FvRZImoOfGtHTOnZaglxfVoOemxykvM78DApZ8dx9fbHYpxuZj--QuA03lv%2F2RVx1o7h--fFPqdcl9ZrCXDQO45ggaJg%3D%3D", domain="truthsocial.com")

print(client.timeline_home())
