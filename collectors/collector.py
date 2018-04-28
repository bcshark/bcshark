import requests
import json

from websocket import WebSocketApp
from struct import pack_into, unpack_from

from model.market_tick import market_tick
from .utility import *

class collector(object):
    DEFAULT_TIMEOUT_IN_SECONDS = 10

    def __init__(this, settings, market_settings):
        this.settings = settings
        this.market_settings = market_settings
        this.logger = settings['logger']
        this.db_adapter = settings['db_adapter']
        this.cache_manager = settings['cache_manager']
        this.symbols = settings['symbols']
        this.symbols_default = this.symbols["default"]
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

    @property
    def symbols_market(this):
        return this.symbols[this.market_name]

    @property
    def table_market_trades(this):
        return '%s_trades' % this.market_name

    @property
    def table_market_ticks(this):
        return '%s_ticks' % this.market_name

    @property
    def table_market_ticks_usd(this):
        return '%s_ticks_usd' % this.market_name

    def on_raw_close(this, websocket_client):
        print 'on close'

    def on_raw_open(this, websocket_client):
        if this.on_open:
            this.on_open(websocket_client)

    def on_raw_message(this, websocket_client, raw_message):
        if this.on_message:
            this.on_message(websocket_client, raw_message)

    def on_raw_error(this, websocket_client, error):
        print 'on error'
        print error

    def send_ws_message_json(this, json_obj):
        message = json.dumps(json_obj)
        this.send_ws_message(message)

    def send_ws_message(this, message):
        if this.websocket_client:
            this.websocket_client.send(message)

    def http_request_json(this, url, headers, cookies = None):
        try:
            res = requests.get(url, headers = headers, cookies = cookies, timeout = this.DEFAULT_TIMEOUT_IN_SECONDS, allow_redirects = True)

            return res.json()
        except Exception, e:
            print e
            return None

    def start_listen_websocket(this, url, listener):
        if not this.websocket_client:
            this.stop_listen_websocket()

        this.logger.info('Connecting to \033[0;32;40m%s\033[0m websocket interface, url: \033[0;32;40m%s\033[0m' % (this.market_name, this.WS_URL))
        this.websocket_client = WebSocketApp(url, on_open = listener.on_raw_open, on_close = listener.on_raw_close, on_message = listener.on_raw_message, on_error = listener.on_raw_error)
        this.websocket_client.run_forever()

    def stop_listen_websocket(this):
        if this.websocket_client:
            this.websocket_client.close()

    def save_trade(this, symbol_name, trade):
        this.db_adapter.save_trade(this.table_market_trades, this.market_name, symbol_name, trade)

    def save_tick(this, symbol_name, tick):
        if symbol_name == this.symbols_default[0]:
            this.update_cache(symbol_name, tick)

        this.db_adapter.save_tick(this.table_market_ticks, this.market_name, symbol_name, tick)

        # calculate usd prices except btc-usd pair
        if not symbol_name == this.symbols_default[0]:
            tick = this.calculate_usd_prices(tick)

        if tick:
            this.db_adapter.save_tick(this.table_market_ticks_usd, this.market_name, symbol_name, tick)

    def bulk_save_ticks(this, symbol_name, ticks):
        sql = "select max(time), market, symbol from %s where market = '%s' and symbol = '%s' group by market, symbol" % (this.table_market_ticks, this.market_name, symbol_name)
        ret = this.db_adapter.query(sql)

        if ret and ret.has_key('series'):
            latest_timestamp = ret['series'][0]['values'][0][1]
            ticks = filter(lambda x: x.time + x.timezone_offset > latest_timestamp, ticks)

        for tick in ticks:
            this.save_tick(symbol_name, tick)
        #this.db_adapter.bulk_save_ticks(this.market_name, symbol_name, ticks)

    def save_k20_index(this, k20_index):
        this.db_adapter.save_k20_index(k20_index)

    def bulk_save_k20_daily_rank(this, k20_ranks):
        this.db_adapter.bulk_save_k20_daily_rank(k20_ranks)

    def query_k20_daily_rank(this):
        sql = "select max(time), symbol, market_cap_usd from k20_daily_rank group by symbol"
        result = this.db_adapter.query(sql)
        return result

    def query_latest_price(this, symbol_name, startSecond):
        #sql = "select max(time), market, symbol, high, low, open, close from market_ticks where symbol = '%s' and time >= '%s' and time <= '%s' group by market, symbol" % (symbol_name, startSecond, startSecond+60000000000)
        sql = "select max(time), market, symbol, high, low, open, close from market_ticks where symbol = '%s' group by market, symbol" % (symbol_name)
        result = this.db_adapter.query(sql)
        return result

    def get_generic_symbol_name(this, symbol_name):
        for symbol_index in range(len(this.symbols_market)):
            if this.symbols_market[symbol_index] == symbol_name:
                return this.symbols_default[symbol_index]

        return None
    
    def calculate_usd_prices(this, tick):
        if (not isinstance(tick, market_tick)) and (not isinstance(tick, dict)):
            return None

        if this.cache_manager:
            cached_prices = this.cache_manager.load_market_symbol_tick(this.market_name, this.symbols_default[0])

            if cached_prices:
                if isinstance(tick, market_tick):
                    tick.open = float(tick.open) * float(cached_prices[1])
                    tick.close = float(tick.close) * float(cached_prices[2])
                    tick.high = float(tick.high) * float(cached_prices[3])
                    tick.low = float(tick.low) * float(cached_prices[4])
                    return tick
                elif isinstance(tick, dict):
                    tick['open'] = float(tick['open']) * float(cached_prices[1])
                    tick['close'] = float(tick['close']) * float(cached_prices[2])
                    tick['high'] = float(tick['high']) * float(cached_prices[3])
                    tick['low'] = float(tick['low']) * float(cached_prices[4])
                    return tick

        return None

    def update_cache(this, symbol_name, tick):
        if this.cache_manager:
            tick_to_save = None

            if isinstance(tick, market_tick):
                tick_to_save = (long(tick.time), float(tick.open), float(tick.close), float(tick.high), float(tick.low))
            elif isinstance(tick, dict):
                tick_to_save = (long(tick['time']), float(tick['open']), float(tick['close']), float(tick['high']), float(tick['low']))

            this.cache_manager.save_market_symbol_tick(this.market_name, symbol_name, tick_to_save)
