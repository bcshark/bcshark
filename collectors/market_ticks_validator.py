import requests
import datetime

from model.market_tick import  market_tick
from .collector import collector
from .utility import *

class market_ticks_validator(collector):
    DEFAULT_PERIOD = 1

    @property
    def market_name(this):
        return "market_ticks_validator"

    def __init__(this, settings, market_settings):
        super(market_ticks_validator, this).__init__(settings, market_settings)
        this.period = this.DEFAULT_PERIOD

    def translate(this, objs):
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
            print('validation - market tick generated', tick.time, tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close, tick.volume, tick.period, tick.timezone_offset)
            ticks.append(tick)

        return ticks

    def collect_rest(this):
        time_second = int(time.time())
        time_second = time_second - (time_second % 60) - 60 + this.timezone_offset
        print time_second
        result = this.query_market_ticks_for_validation(time_second, this.DEFAULT_PERIOD)
        if result == None:
            print 'Error! validation fail due to no price found in this minute'
            return
        ticks = this.translate(result[0]['values'])
        symbol_dict = this.symbols_all_market

        for key in symbol_dict:
            if key != 'default' and key != '_title' and key != 'k10_daily_rank' and key != 'bittrex' and key != 'bitfinex' and key != 'bitstamp':
                symbols = symbol_dict[key]
                for symbol in symbols:
                    if symbol == '':
                        continue
                    count = 0
                    symbol = this.get_generic_symbol_name(key, symbol)
                    print symbol
                    for tick in ticks:
                        if tick.market == key and tick.symbol == symbol:
                            count = count + 1
                    if count == 0:
                        print('error! symbol price missing: ', key, symbol, count)
                    elif count > 1:
                        print('error! symbol price duplicate: ', key, symbol, count)
                    else:
                        print('validation passed for symbol: ', key, symbol, count)

        this.logger.info('validation done!')

    def get_generic_symbol_name(this, key, symbol_name):
        for symbol_index in range(len(this.symbols_all_market[key])):
            if this.symbols_all_market[key][symbol_index] == symbol_name:
                return this.symbols_all_market['default'][symbol_index]