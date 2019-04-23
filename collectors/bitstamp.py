import requests
import json
import re
import threading

from model.market_tick import  market_tick
from .collector import collector
from lib.utility import *

class collector_bitstamp(collector):
    DEFAULT_PERIOD = "1min"
    DEFAULT_SIZE = 200
    PATTERN_LIVE_TRADES = "live_trades([_a-zA-Z]*)"
    CALCULATE_BACK_MINUTES = 5 

    @property
    def market_name(this):
        return "bitstamp"

    def __init__(this, settings, market_settings):
        super(collector_bitstamp, this).__init__(settings, market_settings)

        this.period = this.DEFAULT_PERIOD

        this.start_tick_calculator()

    def start_tick_calculator(this):
        this.calculator_timer = threading.Timer(60, this.calculate_tick)
        this.calculator_timer.start()

    def calculate_tick(this):
        timestamp = long(time.time())
        start_timestamp = timestamp - timestamp % 60 - this.CALCULATE_BACK_MINUTES * 60

        try:
            for begin_timestamp in range(start_timestamp, timestamp, 60):
                this.calculate_1min_tick(begin_timestamp)
        except Exception, e:
            print e
        finally:
            this.start_tick_calculator()

    def calculate_1min_tick(this, begin_timestamp):
        end_timestamp = begin_timestamp + 60
        sql = 'select first(price) as open, last(price) as close, max(price) as high, min(price) as low, sum(amount) as volume from %s where time >= %d and time < %d group by symbol' % (this.table_market_trades, begin_timestamp * 1e9, end_timestamp * 1e9)
        ret = this.db_adapter.query(sql, epoch = 's')

        if ret and ret.has_key('series'):
            for series in ret['series']:
                generic_symbol_name = series['tags']['symbol']
                tick = this.translate_tick(series['values'][0])
                this.save_tick(generic_symbol_name, tick)

    def translate_tick(this, obj):
        tick = market_tick()

        tick.time = long(obj[0])
        tick.timezone_offset = this.timezone_offset
        tick.open = float(obj[1])
        tick.close = float(obj[2])
        tick.high = float(obj[3])
        tick.low = float(obj[4])
        tick.amount = 0.0
        tick.volume = float(obj[5])
        tick.count = 0
        tick.period = this.DEFAULT_PERIOD

        return tick

    def translate_trade(this, ts, obj):
        ret = dict(obj)

        ret['time'] = ts
        ret['timezone_offset'] = this.timezone_offset

        return ret

    def on_open(this, websocket_client, name):
        this.logger.info('bitstamp websocket connection established')

    def on_message(this, websocket_client, raw_message, name):
        #this.logger.info('receive message from bitstamp websocket: %s', raw_message)
        this.logger.info('receive message from bitstamp websocket: (hide)')

        message_json = json.loads(raw_message)

        event = message_json['event']
        data = json.loads(message_json['data'])

        if event == 'pusher:connection_established':
            this.socket_id = data['socket_id']

            for symbol in this.symbols_market:
                if symbol == '':
                    continue

                if symbol == 'btcusd':
                    channel_name = "live_trades"
                else:
                    channel_name = "live_trades_%s" % symbol

                subscription_msg = { "event": "pusher:subscribe", "data": { "channel": channel_name } }
                this.send_ws_message_json(subscription_msg)
        elif event == 'trade':
            channel = message_json['channel']
            match = re.search(this.PATTERN_LIVE_TRADES, channel)

            if match:
                symbol_name = match.group(1)[1:]
            else:
                symbol_name = 'btcusd'

            trade = this.translate_trade(long(data['timestamp']), data)
            this.save_trade(this.get_generic_symbol_name(symbol_name), trade)

    def collect_ws(this, name = '*'):
        this.start_listen_websocket(this.WS_URL, this, 'default')
