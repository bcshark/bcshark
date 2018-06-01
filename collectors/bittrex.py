import json
import requests
import re
import threading

from zlib import decompress, MAX_WBITS
from base64 import b64decode
from requests import Session
from signalr import Connection
from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class collector_bittrex(collector):
    DEFAULT_PERIOD = "1min"
    CALCULATE_BACK_MINUTES = 5 

    @property
    def market_name(this):
        return "bittrex"

    def __init__(this, settings, market_settings):
        super(collector_bittrex, this).__init__(settings, market_settings)

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
        sql = 'select first(l) as open, last(l) as close, max(H) as high, min(L) as low, sum(V) as volume from %s where time >= %d and time < %d group by symbol' % (this.table_market_trades, begin_timestamp * 1e9, end_timestamp * 1e9)
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

        ret['time'] = long(ts / 1000)
        ret['timezone_offset'] = this.timezone_offset

        return ret

    def on_open(this, websocket_client):
        this.logger.info('bittrex websocket connection established')

    def on_message(this, raw_message):
        try:
            raw_message = decompress(b64decode(raw_message), -MAX_WBITS)
        except SyntaxError:
            raw_message = decompress(b64decode(raw_message))

        #this.logger.info('receive message from bittrex websocket: %s', raw_message)
        this.logger.info('receive message from bittrex websocket: (hide)')
        message_json = json.loads(raw_message)

        for tick in message_json['D']:
            if tick['M'] in this.symbols_market:
                symbol_name = tick['M']
                this.save_trade(this.get_generic_symbol_name(symbol_name), this.translate_trade(tick['T'], tick))

    def collect_ws(this):
        with Session() as session:
            connection = Connection(this.WS_URL, session = session)
            hub = connection.register_hub('c2')
            connection.start()

            hub.client.on('uS', this.on_message)
            hub.server.invoke('SubscribeToSummaryDeltas')

            connection.wait()

    def collect_rest(this):
        pass
