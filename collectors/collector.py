import requests
import StringIO
import gzip
import json

from websocket import WebSocketApp

from .utility import *

class collector(object):
    DEFAULT_TIMEOUT_IN_SECONDS = 1;

    def __init__(this, settings, market_settings):
        this.settings = settings
        this.market_settings = market_settings
        this.logger = settings['logger']
        this.db_adapter = settings['db_adapter']
        this.symbols = settings['symbols']
        this.timezone_offset = settings['timezone_offset']
        this.websocket_client = None

    @property
    def REST_URL(this):
        if this.market_settings.has_key('api') and this.market_settings['api'].has_key('rest'):
            return this.market_settings['api']['rest']
        return None

    @property
    def WS_URL(this):
        if this.market_settings.has_key('api') and this.market_settings['api'].has_key('ws'):
            return this.market_settings['api']['ws']
        return None

    def on_close(this, websocket_client):
        print 'on close'

    def on_raw_open(this, websocket_client):
        if this.on_open:
            this.on_open(websocket_client)

    def on_raw_message(this, websocket_client, raw_message):
        if this.on_message:
            with gzip.GzipFile(fileobj = StringIO.StringIO(raw_message), mode = 'rb') as f:
                message = f.read()

            this.on_message(websocket_client, message)

    def on_error(this, websocket_client, error):
        print 'on error'
        print error

    def send_ws_message_json(this, json_obj):
	message = json.dumps(json_obj)
	this.send_ws_message(message)

    def send_ws_message(this, message):
        if this.websocket_client:
            this.websocket_client.send(message)

    def http_request_json(this, url, headers):
        try:
            res = requests.get(url, headers = headers, timeout = this.DEFAULT_TIMEOUT_IN_SECONDS)

            return res.json()
        except Exception, e:
            return None

    def start_listen_websocket(this, url, listener):
        if not this.websocket_client:
            this.stop_listen_websocket()

        this.websocket_client = WebSocketApp(url, on_open = listener.on_open, on_close = listener.on_close, on_message = listener.on_raw_message, on_error = listener.on_error)
        this.websocket_client.run_forever()

    def stop_listen_websocket(this):
        if this.websocket_client:
            this.websocket_client.close()

    def save_tick(this, market_name, symbol_name, tick):
        this.db_adapter.save_tick(market_name, symbol_name, tick)

    def bulk_save_ticks(this, market_name, symbol_name, ticks):
        sql = "select max(time), market, symbol from market_ticks where market = '%s' and symbol = '%s' group by market, symbol" % (market_name, symbol_name)
        ret = this.db_adapter.query(sql)

        if ret and ret.has_key('series'):
            ret = ret['series'][0]['values'][0][1]

            ticks = filter(lambda x: x.time + x.timezone_offset > ret, ticks)

        this.db_adapter.bulk_save_ticks(market_name, symbol_name, ticks)

