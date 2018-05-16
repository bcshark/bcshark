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
            #print('validation - market tick generated', tick.time, tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close, tick.volume, tick.period, tick.timezone_offset)
            ticks.append(tick)

        return ticks

    def collect_rest(this):
        time_second = int(time.time()) #TODO: verify time format and timezone should be UTC
        start_second = time_second - (time_second % 60) - 600 + this.timezone_offset
        end_second = time_second - (time_second % 60) - 120 + this.timezone_offset
        this.logger.info('validation start with range: %s, %s', start_second, end_second)
        symbol_dict = this.symbols_all_market
        for key in symbol_dict:
            # if key != 'default' and key != '_title' and key != 'k10_daily_rank' and key != 'bittrex' and key != 'bitfinex' and key != 'bitstamp':
            if key == 'okcoin' or key == 'gdax' or key == 'gateio':
                symbols = symbol_dict[key]
                for symbol in symbols:
                    if symbol == '':
                        continue
                    generic_symbol = this.get_generic_symbol_name(key, symbol)
                    #print('generic symbol: ', generic_symbol)
                    result = this.query_market_ticks_for_validation(start_second * this.MULTIPLIER, end_second * this.MULTIPLIER, key, generic_symbol)
                    if result == None:
                        this.logger.info('validation error! no price found in time range: %s, %s, %s, %s', key, generic_symbol, start_second, end_second)
                        # validation_s = validation()
                        # validation_s.time = start_second
                        # validation_s.market = key
                        # validation_s.symbol = generic_symbol
                        # validation_s.period = 'time range'
                        # validation_s.timezone_offset = this.timezone_offset
                        # validation_s.table = 'market_ticks'
                        # validation_s.msg = 'miss time range price'
                        # this.save_validation(validation_s)
                        # break
                    else:
                        ticks = this.translate(result[0]['values'])
                        period = temp_time = 0
                        if ticks[0].period == '1min':
                            period = 60
                            temp_time = start_second
                        else:
                            period = 300
                            temp_time = ticks[0].time #TODO: need to search from DB with market and time range then find the min time for poloniex
                        global validation_s
                        for tick in ticks:
                            #print('+++++', tick.symbol, tick.market, tick.time, temp_time)
                            while tick.time > temp_time:
                                validation_s = validation()
                                validation_s.time = temp_time
                                validation_s.market = key
                                validation_s.symbol = generic_symbol
                                validation_s.period = tick.period
                                validation_s.timezone_offset = tick.timezone_offset
                                validation_s.table = 'market_ticks'
                                validation_s.msg = 'miss 1min price'
                                #print('validation fail for', validation_s.time, validation_s.market, validation_s.symbol)
                                this.save_validation(validation_s)
                                temp_time = temp_time + period
                            temp_time = temp_time + period

        this.logger.info('validation done!')

    def get_generic_symbol_name(this, key, symbol_name):
        for symbol_index in range(len(this.symbols_all_market[key])):
            if this.symbols_all_market[key][symbol_index] == symbol_name:
                return this.symbols_all_market['default'][symbol_index]