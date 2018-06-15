import requests
import datetime

from model.market_tick import market_tick
from model.k10_index import k10_index
from .collector import collector
from .utility import *

class collector_k30_index_calc(collector):
    DEFAULT_PERIOD = "1min"
    MULTIPLY_RATIO = 1
    #BASIC_SYMBOL = ['ipfs','ada','eos','zil','ont','elf','ae','knc','agi','mana','powr','eng','theta','iost','blz','ela','brd','icx','ddd','zrx','cvc','wan','aion','drgn','rdn','mobius','ruff','gnx','lrc','snt','ven','nano','wtc','gnt','loom']
    BASIC_SYMBOL = ['zil','ont','elf','ae','knc','agi','mana','powr','eng','theta','iost','blz','ela','brd','icx','ddd','zrx','cvc','wan','aion','drgn','rdn','mobi','ruff','gnx','lrc','snt','ven','nano','wtc','gnt','loom']

    @property
    def market_name(this):
        return "k30_index_calc"

    def __init__(this, settings, market_settings):
        super(collector_k30_index_calc, this).__init__(settings, market_settings)

    def translate_ticks(this, objs):
        ticks = []
        for obj in objs:
            tick = market_tick()
            tick.market = obj['values'][0][1]
            tick.symbol = obj['values'][0][2]
            tick.time = long(obj['values'][0][0])
            tick.high = float(obj['values'][0][3])
            tick.low = float(obj['values'][0][4])
            tick.open = float(obj['values'][0][5])
            tick.close = float(obj['values'][0][6])
            tick.volume = float(obj['values'][0][7])
            tick.amount = float(obj['values'][0][8])
            ticks.append(tick)
            this.logger.debug('k30 calc - Tick object generated from DB query: %s, %s, %s, %s, %s, %s, %s, %s, %s', tick.time, tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close, tick.volume, tick.amount)
        this.logger.debug('k30 calc - length of Tick object generated from DB query: %s', len(ticks))
        return ticks

    def collect_rest(this):
        db_re_gen_flag = this.query_db_re_gen_table()
        if db_re_gen_flag is not None and db_re_gen_flag[0]['values'][0][1] == 'true':
            this.logger.info('start to RE Generate k30 index !')
            this.re_generate_index()
            this.update_db_re_gen_table_false()
        else:
            this.logger.info('start to Calculate k30 index !')
            start_second = this.getStartSecondPreviousMinute()
            this.generate_index(start_second)

    def re_generate_index(this):
        re_gen_start = 1526371200  # start from GMT 2018-05-15 08:00
        re_gen_end = this.getStartSecondPreviousMinute()  # current server time second - 10 minutes
        while re_gen_start <= re_gen_end:
            this.logger.info('++++++ generating index for time second: %s', re_gen_start)
            this.generate_index(re_gen_start)
            re_gen_start = re_gen_start + 60

    def generate_index(this, start_second):

        index = k10_index()
        total_high_weight = total_low_weight = total_open_weight = total_close_weight = 0
        total_volume_weight = 0
        miss_price_market = []
        cal_length = len(this.BASIC_SYMBOL)

        for basic_symbol in this.BASIC_SYMBOL:
            ticks = []
            convert_symbol = this.getSymbol(basic_symbol)
            this.logger.debug('%s, %s, %s', convert_symbol[0], convert_symbol[1], convert_symbol[2])
            tick_result = this.query_previous_min_price(convert_symbol[0], convert_symbol[1], convert_symbol[2], start_second)
            if tick_result == None:
                miss_price_market = this.find_miss_price_market(convert_symbol, None)
            else:
                ticks = this.translate_ticks(tick_result)
                miss_price_market = this.find_miss_price_market(convert_symbol, ticks)
            for miss_market in miss_price_market:
                tick_result_exist = this.query_latest_price_exist(convert_symbol[0], convert_symbol[1], convert_symbol[2], miss_market, start_second)
                if tick_result_exist == None:
                    continue
                miss_ticks = this.translate_ticks(tick_result_exist)
                ticks.extend(miss_ticks)
            if len(ticks) == 0:
                this.logger.error('k30 calc Error - No price found, bypass this symbol: %s, %s, %s ', convert_symbol[0], convert_symbol[1], convert_symbol[2])
                cal_length = cal_length - 1
                continue
            filtered_ticks = this.get_filtered_ticks(ticks, basic_symbol)
            total_high_weight = total_high_weight + this.calculate_symbol_price(filtered_ticks, 'high', basic_symbol)
            total_low_weight = total_low_weight + this.calculate_symbol_price(filtered_ticks, 'low', basic_symbol)
            total_open_weight = total_open_weight + this.calculate_symbol_price(filtered_ticks, 'open', basic_symbol)
            total_close_weight = total_close_weight +  this.calculate_symbol_price(filtered_ticks, 'close', basic_symbol)
            sum_volume = this.calculate_symbol_volume(filtered_ticks, basic_symbol)
            total_volume_weight = total_volume_weight + sum_volume

        if cal_length == 0:
            this.logger.error('k30 calc Error - validated symbol weight is 0 ! program exit')
            return
        index.timezone_offset = this.timezone_offset
        index.high = total_high_weight * this.MULTIPLY_RATIO
        index.low = total_low_weight * this.MULTIPLY_RATIO
        index.open = total_open_weight * this.MULTIPLY_RATIO
        index.close = total_close_weight * this.MULTIPLY_RATIO
        index.volume = total_volume_weight
        index.time = start_second
        index.period = '1min'

        this.logger.debug('k30 calc - index.timezone_offset: %s', index.timezone_offset)
        this.logger.debug('k30 calc - index.high: %s', index.high)
        this.logger.debug('k30 calc - index.low: %s', index.low)
        this.logger.debug('k30 calc - index.open: %s', index.open)
        this.logger.debug('k30 calc - index.close: %s', index.close)
        this.logger.debug('k30 calc - index.volume: %s', index.volume)
        this.logger.debug('k30 calc - index.time: %s', index.time)
        this.logger.debug('k30 calc - index.period: %s', index.period)

        indexObj = {
            "time": index.time,
            "timezone_offset": index.timezone_offset,
            "open": index.open,
            "close": index.close,
            "volume": index.volume,
            "low": index.low,
            "high": index.high,
            "period": index.period
        }
        indexArray = [indexObj]
        if index.high != 0 and index.low != 0 and index.open != 0 and index.close != 0:
            this.save_index('k30_index', indexArray[0])
        else:
            this.logger.error('k30 Calc Error - index price is 0! %s, %s, %s, %s ', index.high, index.low, index.open, index.close)

        this.logger.info('k30 index calc done !')

    def getStartSecondPreviousMinute(this):
        time_second = int(time.time())
        time_second = time_second - (time_second % 60) - 600 + this.timezone_offset
        this.logger.debug('k30 calc - start second generated: %s', time_second)
        return time_second;

    def fillRatio(this, ranks):
        total_cap = 0
        for rank in ranks:
            total_cap = total_cap + rank.market_cap_usd
        for rank in ranks:
            rank.cap_ratio = rank.market_cap_usd / total_cap
            this.logger.debug('k30 calc - rank object symbol: %s  Cap Ratio: %s', rank.symbol, rank.cap_ratio)
        return ranks


    def calculate_symbol_price(this, filtered_ticks, price_field, base_symbol):

        sum_price = 0
        for key in filtered_ticks.keys():
            tick = filtered_ticks[key]
            this.logger.debug('filtered tick in calc avg price: %s, %s, %s, %s, %s, %s, %s ', key, tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close)
            if price_field == 'high':
                sum_price = sum_price + tick.high
            if price_field == 'low':
                sum_price = sum_price + tick.low
            if price_field == 'open':
                sum_price = sum_price + tick.open
            if price_field == 'close':
                sum_price = sum_price + tick.close
        avg_price = (sum_price / len(filtered_ticks))
        this.logger.debug('k30 calc - Calculated avg price %s Is: %s for symbol: %s', price_field, avg_price, base_symbol)
        return avg_price

    def calculate_symbol_volume(this, filtered_ticks, base_symbol):

        sum_volume = 0
        for key in filtered_ticks.keys():
            tick = filtered_ticks[key]
            sum_volume = sum_volume + ((tick.high + tick.low) / 2 * tick.volume)
            this.logger.debug('filtered tick in calc total volumne: %s, %s, %s, %s, %s, %s, %s ', tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close, tick.volume)
        this.logger.debug('k30 calc volume - %s for symbol: %s', sum_volume, base_symbol)
        return sum_volume

    def get_filtered_ticks(this, ticks, base_symbol):
        filtered_ticks = {}
        for tick in ticks:
            tick_key = tick.market + base_symbol
            if tick_key not in filtered_ticks.keys():
                filtered_ticks[tick_key] = tick
            elif 'usdt' in tick.symbol or ('usdt' not in filtered_ticks[tick_key].symbol and 'btc' in tick.symbol):
                filtered_ticks[tick_key] = tick
        return filtered_ticks

    def find_miss_price_market(this, symbols, ticks): ###TODO: miss check the others, if only one usdt exists then pass ? how about eth and btc ?
        if symbols is None:
            return None
        miss_market = []
        symbol_usdt = symbols[0]
        symbol_btc = symbols[1]
        symbol_eth = symbols[2]
        index_usdt = this.symbols_all_market['default'].index(symbol_usdt)
        index_btc = this.symbols_all_market['default'].index(symbol_btc)
        index_eth = this.symbols_all_market['default'].index(symbol_eth)
        this.logger.info('symbol_usdt, %s, symbol_btc, %s, symbol_eth, %s', symbol_usdt, symbol_btc, symbol_eth)
        valid_markets = this.find_all_valid_markets()
        if ticks == None:
            for key in valid_markets:
                this.logger.info('%s: %s, %s, %s', key, this.symbols_all_market[key][index_usdt], this.symbols_all_market[key][index_btc], this.symbols_all_market[key][index_eth])
                if this.symbols_all_market[key][index_usdt] != '' or this.symbols_all_market[key][index_btc] != '' or this.symbols_all_market[key][index_eth] != '':
                    miss_market.append(key)
        else:
            for key in valid_markets:
                this.logger.info('%s: %s, %s, %s', key, this.symbols_all_market[key][index_usdt], this.symbols_all_market[key][index_btc], this.symbols_all_market[key][index_eth])
                market_exist = False
                for tick in ticks:
                    if tick.market == key:
                        market_exist = True
                        break
                if not market_exist:
                    if this.symbols_all_market[key][index_usdt] != '' or this.symbols_all_market[key][index_btc] != '' or this.symbols_all_market[key][index_eth] != '':
                        miss_market.append(key)
        return miss_market

    def find_all_valid_markets(this):
        valid_markets = []
        symbol_dict = this.symbols_all_market
        for key in symbol_dict:
            if key != 'default' and key != '_title' and key != 'k10_daily_rank' and key != 'bittrex' and key != 'bitfinex' and key != 'bitstamp':
            #if key != 'default' and key != '_title' and key != 'k10_daily_rank' and key != 'bittrex':
                valid_markets.append(key)
        return valid_markets

    def getSymbol(this, basic_symbol):
        return [basic_symbol + 'usdt', basic_symbol + 'btc', basic_symbol + 'eth']
