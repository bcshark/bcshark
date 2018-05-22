import requests
import datetime

from model.market_tick import  market_tick
from .collector import collector
from .utility import *
from model.validation import validation

class market_ticks_validator(collector):
    MULTIPLIER = 1000000000

    @property
    def market_name(this):
        return "market_ticks_validator"

    def __init__(this, settings, market_settings):
        super(market_ticks_validator, this).__init__(settings, market_settings)

    def translate_db(this, objs):
        ticks = []
        for obj in objs:
            tick = market_tick()
            tick.time = long(obj[0])
            tick.market = obj[1]
            tick.symbol = obj[2]
            tick.high = float(obj[3])
            tick.low = float(obj[4])
            tick.open = float(obj[5])
            tick.close = float(obj[6])
            tick.volume = float(obj[7])
            tick.period = obj[8]
            tick.timezone_offset = obj[9]
            # print('validation - market tick generated', tick.time, tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close, tick.volume, tick.period, tick.timezone_offset)
            ticks.append(tick)

        return ticks

    def collect_rest(this):
        time_second = int(time.time()) #TODO: verify time format and timezone should be UTC
        start_second = time_second - (time_second % 60) - 600 + this.timezone_offset
        end_second = time_second - (time_second % 60) - 300 + this.timezone_offset
        this.validation_logger.info('validation start with range: %s, %s', start_second, end_second)
        symbol_dict = this.symbols_all_market
        for key in symbol_dict:
            # if key != 'default' and key != '_title' and key != 'k10_daily_rank' and key != 'bittrex' and key != 'bitfinex' and key != 'bitstamp':
            if key == 'gdax':
                symbols = symbol_dict[key]
                for symbol in symbols:
                    if symbol == '':
                        continue
                    generic_symbol = this.get_generic_symbol_name(key, symbol)
                    result = this.query_market_ticks_for_validation(start_second * this.MULTIPLIER, end_second * this.MULTIPLIER, key, generic_symbol)
                    if result is None:
                        this.validation_logger.error('validation error! no price found in time range: %s, %s, %s, %s', key, generic_symbol, start_second, end_second)
                    else:
                        ticks_db = this.translate_db(result[0]['values'])
                        ticks_rest = this.translate_rest(key, symbol);
                        period = temp_time = 0
                        if ticks_db[0].period == '1min':
                            period = 60
                            temp_time = start_second
                        else:
                            period = 300
                            temp_time = ticks_db[0].time #TODO: need to search from DB with market and time range then find the min time for poloniex
                        global validation_s
                        for tick in ticks_db:
                            while tick.time > temp_time:
                                validation_s = validation()
                                validation_s.time = temp_time
                                validation_s.market = key
                                validation_s.symbol = generic_symbol
                                validation_s.period = tick.period
                                validation_s.timezone_offset = tick.timezone_offset
                                validation_s.table = 'market_ticks'
                                validation_s.msg = 'miss usdt price'
                                this.validation_logger.error('fail validation market_ticks miss price of minute: %s, %s, %s ', validation_s.time, validation_s.market, validation_s.symbol)
                                this.save_validation(validation_s)
                                temp_time = temp_time + period
                            temp_time = temp_time + period

                        for tick1 in ticks_db:
                            for tick2 in ticks_rest:
                                if tick2.time == tick1.time - this.timezone_offset:

                                    validation_s = validation()
                                    validation_s.time = tick1.time
                                    validation_s.market = key
                                    validation_s.symbol = generic_symbol
                                    validation_s.period = tick1.period
                                    validation_s.timezone_offset = tick1.timezone_offset

                                    if 'usdt' in tick1.symbol:
                                        if float(format(tick2.high, '.6f')) == float(format(tick1.high, '.6f')) and float(format(tick2.low, '.6f')) == float(format(tick1.low, '.6f')) and float(format(tick2.open, '.6f')) == float(format(tick1.open, '.6f')) and float(format(tick2.close, '.6f')) == float(format(tick1.close, '.6f')) and float(format(tick2.volume, '.6f')) == float(format(tick1.volume, '.6f')):
                                            this.validation_logger.info('pass validation usdt price: %s, %s, %s, %s', tick1.time, key, generic_symbol, tick1.market)
                                            break
                                        else:
                                            validation_s.table = 'market_ticks'
                                            validation_s.msg = 'usdt price not match'
                                            this.save_validation(validation_s)
                                            this.validation_logger.error('fail validation - usdt price not match: %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s', tick1.time, key, generic_symbol, float(format(tick2.high, '.6f')), float(format(tick1.high, '.6f')), float(format(tick2.low, '.6f')), float(format(tick1.low, '.6f')), float(format(tick2.open, '.6f')), float(format(tick1.open, '.6f')), float(format(tick2.close, '.6f')), float(format(tick1.close, '.6f')), float(format(tick2.volume, '.6f')), float(format(tick1.volume, '.6f')))
                                            break
                                    else:
                                        table_name = key + '_ticks'
                                        db_query = this.query_ticks_table_for_validation(table_name, tick1.time * this.MULTIPLIER, key, generic_symbol)
                                        if result is None:
                                            this.validation_logger.error('fail validation - miss btc price & btcusdt price: %s, %s, %s, %s ', tick1.time, key, generic_symbol, table_name)
                                            validation_s.table = table_name
                                            validation_s.msg = 'miss btc price & btcusdt price'
                                            this.save_validation(validation_s)
                                            break
                                        else:
                                            ticks_objs = this.translate_db(db_query[0]['values'])
                                            if len(ticks_objs) == 1:
                                                this.validation_logger.error('fail validation - miss btc price or btcusdt price: %s, %s, %s, %s ', tick1.time, key, generic_symbol, table_name)
                                                validation_s.table = table_name
                                                if ticks_objs[0].symbol == 'btcusdt':
                                                    validation_s.msg = 'miss btc price'
                                                else:
                                                    validation_s.msg = 'miss btcusdt price'
                                                this.save_validation(validation_s)
                                                break
                                            btc_tick = market_tick()
                                            tick_obj = market_tick()
                                            if ticks_objs[0].symbol == 'btcusdt':
                                                btc_tick = ticks_objs[0]
                                                tick_obj = ticks_objs[1]
                                            else:
                                                btc_tick = ticks_objs[1]
                                                tick_obj = ticks_objs[0]
                                            if float(format(tick2.high, '.6f')) == float(format(tick_obj.high, '.6f')) and float(format(tick2.low, '.6f')) == float(format(tick_obj.low, '.6f')) and float(format(tick2.open, '.6f')) == float(format(tick_obj.open, '.6f')) and float(format(tick2.close, '.6f')) == float(format(tick_obj.close, '.6f')) and float(format(tick2.volume, '.6f')) == float(format(tick_obj.volume, '.6f')):
                                                if float(format(tick1.high, '.6f')) == float(format(tick_obj.high * btc_tick.high, '.6f')) and float(format(tick1.low, '.6f')) == float(format(tick_obj.low * btc_tick.low, '.6f')) and float(format(tick1.open, '.6f')) == float(format(tick_obj.open * btc_tick.open, '.6f')) and float(format(tick1.close, '.6f')) == float(format(tick_obj.close * btc_tick.close, '.6f')) and float(format(tick1.volume, '.6f')) == float(format(tick_obj.volume, '.6f')):
                                                    this.validation_logger.info('pass validation btc convert usdt price: %s, %s, %s, %s', tick1.time, key, symbol, table_name)
                                                    break
                                                else:
                                                    this.validation_logger.error('fail validation - btc convert usdt price not match: %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s', tick1.time, key, generic_symbol, 'market_ticks', float(format(tick1.high, '.6f')), float(format(tick_obj.high * btc_tick.high, '.6f')), float(format(tick1.low, '.6f')), float(format(tick_obj.low * btc_tick.low, '.6f')), float(format(tick1.open, '.6f')), float(format(tick_obj.open * btc_tick.open, '.6f')), float(format(tick1.close, '.6f')), float(format(tick_obj.close * btc_tick.close, '.6f')), float(format(tick1.volume, '.6f')), float(format(tick_obj.volume, '.6f')))
                                                    validation_s.table = 'market_ticks'
                                                    validation_s.msg = 'btc convert usdt price not match'
                                                    this.save_validation(validation_s)
                                                    break
                                            else:
                                                this.validation_logger.error('fail validation - btc price not match: %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s', tick1.time, key, generic_symbol, table_name, float(format(tick2.high, '.6f')), float(format(tick_obj.high, '.6f')), float(format(tick2.low, '.6f')), float(format(tick_obj.low, '.6f')), float(format(tick2.open, '.6f')), float(format(tick_obj.open, '.6f')), float(format(tick2.close, '.6f')), float(format(tick_obj.close, '.6f')), float(format(tick2.volume, '.6f')), float(format(tick_obj.volume, '.6f')))
                                                validation_s.table = table_name
                                                validation_s.msg = 'btc price not match'
                                                this.save_validation(validation_s)
                                                break


        this.validation_logger.info('validation done!')

    def get_generic_symbol_name(this, key, symbol_name):
        for symbol_index in range(len(this.symbols_all_market[key])):
            if this.symbols_all_market[key][symbol_index] == symbol_name:
                return this.symbols_all_market['default'][symbol_index]

    def translate_rest(this, key, symbol):
        if key == 'huobi':
            url = "https://api.huobi.pro/market/history/kline?peroid=1min&size=60&symbol=%s" % (symbol)
            rest = this.http_request_json(url, None)
            if not rest or not rest.has_key('data'):
                this.validation_logger.error('validation cannot get response from huobi (%s)' % symbol)
                return None
            ticks = []
            for obj in rest['data']:
                tick = market_tick()
                tick.time = long(obj['id'])
                tick.open = float(obj['open'])
                tick.close = float(obj['close'])
                tick.low = float(obj['low'])
                tick.high = float(obj['high'])
                tick.amount = float(obj['vol'])
                tick.volume = float(obj['amount'])
                tick.count = float(obj['count'])
                ticks.append(tick)
            return ticks
        if key == 'binance':
            url = "https://api.binance.com/api/v1/klines?symbol=%s&interval=1m&limit=60" % (symbol)
            rest = this.http_request_json(url, None)
            if not rest or not isinstance(rest, list):
                this.validation_logger.error('validation cannot get response from binance (%s)' % symbol)
                return None
            ticks = []
            for obj in rest:
                tick = market_tick()
                tick.time = long(obj[0] / 1000)
                tick.open = float(obj[1])
                tick.close = float(obj[4])
                tick.low = float(obj[3])
                tick.high = float(obj[2])
                tick.amount = float(obj[7])
                tick.volume = float(obj[5])
                tick.count = float(0)
                ticks.append(tick)
            return ticks
        if key == 'gateio':
            url = "http://data.gateio.io/api2/1/candlestick2/%s?group_sec=60&range_hour=1" % (symbol)
            rest = this.http_request_json(url, None)
            if not rest or not isinstance(rest, dict):
                this.validation_logger.error('validation cannot get response from gateio (%s)' % symbol)
                return None
            ticks = []
            for obj in rest["data"]:
                tick = market_tick()
                tick.time = long(obj[0])/1000
                tick.volume = float(obj[1])
                tick.close = float(obj[2])
                tick.high = float(obj[3])
                tick.low = float(obj[4])
                tick.open = float(obj[5])
                tick.amount = 0.0
                tick.count = 0.0
                ticks.append(tick)
            return ticks
        if key == 'gdax':
            start = (datetime.datetime.utcnow() - datetime.timedelta(minutes = 12)).strftime('%Y-%m-%dT%H:%MZ')
            end = (datetime.datetime.utcnow()).strftime('%Y-%m-%dT%H:%MZ')
            url = "https://api.gdax.com/products/%s/candles?start=%s&end=%s&granularity=60" % (symbol, start, end)
            rest = this.http_request_json(url, None)
            if not rest or not isinstance(rest, list):
                this.validation_logger.error('validation cannot get response from gdax (%s)' % symbol)
                return None
            ticks = []
            for obj in rest:
                tick = market_tick()
                tick.time = long(obj[0])
                tick.open = float(obj[3])
                tick.high = float(obj[2])
                tick.low = float(obj[1])
                tick.close = float(obj[4])
                tick.volume = float(obj[5])
                tick.amount = 0.0
                tick.count = 0.0
                ticks.append(tick)
                #print(tick.time,tick.high,tick.low,tick.open,tick.low,tick.volume,)
            return ticks
        if key == 'okcoin':
            url = "https://www.okcoin.com/api/v1/kline.do?symbol=%s&type=1min&size=60" % (symbol)
            rest = this.http_request_json(url, None)
            if not rest or not isinstance(rest, list):
                this.validation_logger.error('validation cannot get response from okcoin (%s)' % symbol)
                return None
            ticks = []
            for obj in rest:
                tick = market_tick()
                tick.time = long(obj[0] / 1000)
                tick.open = float(obj[1])
                tick.high = float(obj[2])
                tick.low = float(obj[3])
                tick.close = float(obj[4])
                tick.volume = float(obj[5])
                tick.amount = 0.0
                tick.count = 0.0
                ticks.append(tick)
            return ticks
        if key == 'okex':
            url = "https://www.okex.com/api/v1/future_kline.do?symbol=%s&type=1min&contract_type=this_week&size=60" % (symbol)
            rest = this.http_request_json(url, None)
            if not rest or not isinstance(rest, list):
                this.validation_logger.error('validation cannot get response from okex (%s)' % symbol)
                return None
            ticks = []
            for obj in rest:
                tick = market_tick()
                tick.time = long(obj[0] / 1000)
                tick.timezone_offset = this.timezone_offset
                tick.open = float(obj[1])
                tick.high = float(obj[2])
                tick.low = float(obj[3])
                tick.close = float(obj[4])
                tick.volume = float(obj[6])
                tick.amount = float(obj[5])
                tick.count = 0.0
                ticks.append(tick)
            return ticks
        if key =='poloniex':
            time_second = int(time.time())
            time_second = time_second - time_second % 60 - 3600
            url = "https://poloniex.com/public?command=returnChartData&currencyPair=%s&start=%s&period=300" % (symbol, time_second)
            rest = this.http_request_json(url, None)
            if not rest or not isinstance(rest, list):
                this.validation_logger.error('validation cannot get response from poloniex (%s)' % symbol)
                return None
            ticks = []
            for obj in rest:
                tick = market_tick()
                tick.time = long(obj["date"])
                tick.open = float(format(obj["open"], '.12f'))
                tick.high = float(format(obj["high"], '.12f'))
                tick.low = float(format(obj["low"], '.12f'))
                tick.close = float(format(obj["close"], '.12f'))
                tick.volume = float(format(obj["quoteVolume"], '.12f'))
                tick.amount = float(format(obj["volume"], '.12f'))
                tick.count = 0.0
                ticks.append(tick)
            return ticks