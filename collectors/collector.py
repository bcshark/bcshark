import requests

class collector(object):
    def __init__(this, logger):
        this.logger = logger

    def http_request_json(this, url):
        try:
            res = requests.get(url)

            return res.json()
        except Exception, e:
            return None
