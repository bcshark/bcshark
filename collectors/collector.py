import requests
import MySQLdb


from .utility import *

class collector(object):
    def __init__(this, settings):
        this.settings = settings
        this.logger = settings['logger']
        this.db_adapter = settings['db_adapter']
        this.symbols = settings['symbols']
        this.timezone_offset = settings['timezone_offset']

    def get_mysql_client(this):
        if not this.client:
            mysql_conf = this.settings['mysqldb']
            this.client = MySQLdb.connect(host = mysql_conf['host'], port = mysql_conf['port'], database = mysql_conf['database'], user = mysql_conf['username'], passwd = mysql_conf['password'])

        return this.client

    def http_request_json(this, url, headers):
        try:
            res = requests.get(url, headers = headers)

            return res.json()
        except Exception, e:
            return None

    def bulk_save_ticks(this, market_name, symbol_name, ticks):
        this.db_adapter.bulk_save_ticks(market_name, symbol_name, ticks)

