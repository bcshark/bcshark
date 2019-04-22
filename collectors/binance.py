import requests
import json
import re

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_binance(collector):
    DEFAULT_PERIOD = "1m"
    DEFAULT_SIZE = 60
    PATTERN_KLINE = "([a-zA-Z]+)@kline_[0-9]+\w"
    PATTERN_TRADE = "([a-zA-Z]+)@trade"

    is_subscription_sent = False

    @property
    def market_name(this):
        return "binance"

    def __init__(this, settings, market_settings):
        super(collector_binance, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD

    def translate_rest(this, obj):
        tick = market_tick()
        tick.time = long(obj[0] / 1000)
        tick.timezone_offset = this.timezone_offset
        tick.open = float(obj[1])
        tick.close = float(obj[4])
        tick.low = float(obj[3])
        tick.high = float(obj[2])
        tick.amount = float(obj[7])
        tick.volume = float(obj[5])
        tick.count = float(0)
        tick.period = this.get_generic_period_name(this.period)

        return tick

    def translate_ws(this, obj):
        tick = market_tick()
        tick.time = long(obj['t'] / 1000)
        tick.timezone_offset = this.timezone_offset
        tick.open = float(obj['o'])
        tick.close = float(obj['c'])
        tick.low = float(obj['l'])
        tick.high = float(obj['h'])
        tick.amount = 0.0
        tick.volume = float(obj['v'])
        tick.count = float(obj['n'])
        tick.period = this.get_generic_period_name(this.period)

        return tick

    def translate_trade_ws(this, obj):
        trade = {}

        trade['timezone_offset'] = this.timezone_offset
        trade['time'] = long(obj['T'] / 1000)
        trade['id'] = obj['t']
        trade['amount'] = obj['q']
        trade['price'] = obj['p']
        trade['type'] = (0 if str(obj['m']) == 'True' else 1)
        trade['timestamp'] = long(obj['T'] / 1000)
        trade['microtimestamp'] = long(obj['T'])
        trade['buy_order_id'] = obj['b']
        trade['sell_order_id'] = obj['a']

        return trade

    def collect_ws(this):
        #streams = "/".join(["%s@kline_1m" % symbol.lower() for symbol in this.symbols_market if symbol != ""])
        #ws_url = "%s/stream?streams=%s" % (this.WS_URL, streams)

        streams = "/".join(["%s@trade" % symbol.lower() for symbol in this.symbols_market if symbol != ""])
        ws_url = "%s/stream?streams=%s" % (this.WS_URL, streams)

        this.start_listen_websocket(ws_url, this)

    def on_open(this, websocket_client):
        this.logger.info('binance websocket connection established')

        this.is_subscription_sent = False

    def on_message(this, websocket_client, raw_message):
        this.logger.debug('receive message from binance websocket: %s', raw_message)
        message_json = json.loads(raw_message)

        if message_json.has_key('ping'):
            this.send_ws_message(json.dumps({ 'pong': message_json['ping'] }))
        elif message_json.has_key('stream'):
            channel = message_json['stream']

            if "kline" in channel:
                match = re.search(this.PATTERN_KLINE, channel)

                if match:
                    symbol_name = match.group(1)
                    this.save_tick(this.get_generic_symbol_name(symbol_name.upper()), this.translate_ws(message_json['data']['k']))
            elif "trade" in channel:
                match = re.search(this.PATTERN_TRADE, channel)

                if match:
                    symbol_name = match.group(1)
                    this.save_trade(this.get_generic_symbol_name(symbol_name.upper()), this.translate_trade_ws(message_json['data']))

    def collect_rest(this):
        starttime = time.time()
        url = this.REST_URL + 'ping'
        ticks = this.http_request_json(url, None)
        elapse = time.time() - starttime

        if ticks or ticks == {}:
            this.save_check(True, elapse)

            for symbol in this.symbols_market:
                if symbol == "":
                    continue

                url = "klines?symbol=%s&interval=%s&limit=%d" % (symbol.upper(), this.DEFAULT_PERIOD, this.DEFAULT_SIZE)
                url = this.REST_URL + url
                ticks = this.http_request_json(url, None)

                if not ticks or not isinstance(ticks, list):
                    this.logger.error('cannot get ticks from binance (%s)' % symbol)
                    continue

                this.save_market_ticks(this.get_generic_symbol_name(symbol), [ this.translate_rest(tick) for tick in ticks ])

                this.logger.info('get ticks from binance')
        else:
            this.save_check(True, elapse)

    def collect_exchange_info(this):
        url = this.REST_URL + 'exchangeInfo'
        market_info = this.http_request_json(url, None)

        if market_info:
            this.save_market_info(market_info)
            this.logger.info('get market info from binance')

    def get_generic_period_name(this, period_name):
        if period_name == '1m':
            return '1min'

        return period_name
