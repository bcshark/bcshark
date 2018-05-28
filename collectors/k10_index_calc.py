import requests
import datetime

from model.market_tick import market_tick
from model.k10_rank import k10_rank
from model.k10_index import k10_index
from .collector import collector
from .utility import *

class collector_k10_index_calc(collector):
    DEFAULT_PERIOD = "1min"
    MULTIPLY_RATIO = 1000
    # BASIC PRICE FOR EACH MARKET AT GMT 2018-05-15 08:00 - 08:01 [HIGH, LOW, OPEN, CLOSE]
    BASIC_PRICE = {
    'BASIC_ADA':[0.273427],
    'BASIC_BTC':[8720.361692494002],
    'BASIC_BCH':[1425.6034000000002],
    'BASIC_ETH':[733.152397074],
    'BASIC_ETC':[19.498168657109375],
    'BASIC_EOS':[14.127700907812502],
    'BASIC_DASH':[442.0082719572917],
    'BASIC_LTC':[146.837522],
    'BASIC_XRP':[0.73803522421875],
    'BASIC_XMR':[215.95556397359377]}

    @property
    def market_name(this):
        return "k10_index_calc"

    def __init__(this, settings, market_settings):
        super(collector_k10_index_calc, this).__init__(settings, market_settings)

    def translate_ranks(this, objs):
        k10_ranks = []
        for obj in objs:
            rank = k10_rank()
            symbol = this.getSymbol(obj['values'][0][1])
            if symbol == None:
                this.logger.debug('k10 calc - Warning: New symbol found on top 20, no price collected, bypass it!: %s', obj['values'][0][1])
                continue
            rank.symbol = symbol
            rank.time = long(obj['values'][0][0])
            rank.market_cap_usd = float(obj['values'][0][2])
            rank.rank = int(obj['values'][0][3])
            if rank.market_cap_usd <= 0:
                this.logger.error('k10 calc Error - k10_daily_rank query result market cap is incorrect! %s, %s', rank.symbol, rank.market_cap_usd)
                continue
            k10_ranks.append(rank)
            this.logger.debug('k10 calc - rank object generated from DB query: %s, %s, %s, %s', rank.time, rank.symbol, rank.market_cap_usd, rank.rank)
        return k10_ranks

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
            this.logger.debug('k10 calc - Tick object generated from DB query: %s, %s, %s, %s, %s, %s, %s, %s, %s', tick.time, tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close, tick.volume, tick.amount)
        this.logger.debug('k10 calc - length of Tick object generated from DB query: %s', len(ticks))
        return ticks

    def collect_rest(this):
        db_re_gen_flag = this.query_db_re_gen_table()
        if db_re_gen_flag is not None and db_re_gen_flag[0]['values'][0][1] == 'true':
            this.logger.info('start to RE Generate k10 index !')
            this.re_generate_index()
            this.update_db_re_gen_table_false()
        else:
            this.logger.info('start to Calculate k10 index !')
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
        rank_result = this.query_k10_daily_rank(start_second)
        if rank_result == None:
            this.logger.error('k10 calc Error - k10_daily_rank table has no data!')
            return
        ranks = this.translate_ranks(rank_result)
        if len(ranks) == 0:
            this.logger.error('k10 calc Error - translate k10 rank object length is 0, program exit %s', rank_result)
            return
        if len(ranks) != 10:
            this.logger.error('k10 calc Error - rank length generated: %s is not 10 program exit!', len(ranks))
            return
        ranks = this.fillRatio(ranks)

        cal_length = len(ranks)
        this.logger.debug('k10 calc - length of rank object generated: %s', cal_length)
        total_high_weight = total_low_weight = total_open_weight = total_close_weight = 0
        total_volume_weight = 0
        miss_price_market = []

        for rank in ranks:
            ticks = []
            this.logger.debug('%s, %s, %s', rank.symbol[0], rank.symbol[1], start_second)
            tick_result = this.query_previous_min_price(rank.symbol[0], rank.symbol[1], start_second)
            if tick_result == None:
                miss_price_market = this.find_miss_price_market(rank.symbol, None)
            else:
                ticks = this.translate_ticks(tick_result)
                miss_price_market = this.find_miss_price_market(rank.symbol, ticks)
            for miss_market in miss_price_market:
                tick_result_exist = this.query_latest_price_exist(rank.symbol[0], rank.symbol[1], miss_market, start_second)
                if tick_result_exist == None:
                    continue
                miss_ticks = this.translate_ticks(tick_result_exist)
                ticks.extend(miss_ticks)
            if len(ticks) == 0:
                this.logger.error('k10 calc Error - No price found, bypass this symbol: %s, %s ', rank.symbol[0], rank.symbol[1])
                cal_length = cal_length - 1 ###TODO: by pass one symbol in index calc need to re-calculate the ratio for each existing symbol again
                continue
            total_high_weight = total_high_weight + this.calculate_symbol_weight(ticks, 'high', rank)
            total_low_weight = total_low_weight + this.calculate_symbol_weight(ticks, 'low', rank)
            total_open_weight = total_open_weight + this.calculate_symbol_weight(ticks, 'open', rank)
            total_close_weight = total_close_weight +  this.calculate_symbol_weight(ticks, 'close', rank)
            sum_volume = this.calculate_symbol_volume(ticks)
            total_volume_weight = total_volume_weight + sum_volume

        if cal_length == 0:
            this.logger.error('k10 calc Error - validated symbol weight is 0 ! program exit')
            return
        index.timezone_offset = this.timezone_offset
        index.high = total_high_weight * this.MULTIPLY_RATIO
        index.low = total_low_weight * this.MULTIPLY_RATIO
        index.open = total_open_weight * this.MULTIPLY_RATIO
        index.close = total_close_weight * this.MULTIPLY_RATIO
        index.volume = total_volume_weight
        index.time = start_second
        index.period = '1min'

        this.logger.debug('k10 calc - index.timezone_offset: %s', index.timezone_offset)
        this.logger.debug('k10 calc - index.high: %s', index.high)
        this.logger.debug('k10 calc - index.low: %s', index.low)
        this.logger.debug('k10 calc - index.open: %s', index.open)
        this.logger.debug('k10 calc - index.close: %s', index.close)
        this.logger.debug('k10 calc - index.volume: %s', index.volume)
        this.logger.debug('k10 calc - index.time: %s', index.time)
        this.logger.debug('k10 calc - index.period: %s', index.period)

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
            this.save_k10_index(indexArray[0])
        else:
            this.logger.error('k10 Calc Error - index price is 0! %s, %s, %s, %s ', index.high, index.low, index.open, index.close)

        this.logger.info('k10 index calc done !')

    def getStartSecondPreviousMinute(this):
        time_second = int(time.time())
        time_second = time_second - (time_second % 60) - 600 + this.timezone_offset
        this.logger.debug('k10 calc - start second generated: %s', time_second)
        return time_second;

    def fillRatio(this, ranks):
        total_cap = 0
        for rank in ranks:
            total_cap = total_cap + rank.market_cap_usd
        for rank in ranks:
            rank.cap_ratio = rank.market_cap_usd / total_cap
            this.logger.debug('k10 calc - rank object symbol: %s  Cap Ratio: %s', rank.symbol, rank.cap_ratio)
        return ranks


    def calculate_symbol_weight(this, ticks, price_field, rank):

        sum_price = 0
        index_field = 'BASIC_' + rank.symbol[0].split('usdt')[0].upper()
        base_price = this.BASIC_PRICE[index_field][0]
        this.logger.debug('basic price name: %s, %s', index_field, base_price)
        for tick in ticks:
            if price_field == 'high':
                sum_price = sum_price + tick.high
            if price_field == 'low':
                sum_price = sum_price + tick.low
            if price_field == 'open':
                sum_price = sum_price + tick.open
            if price_field == 'close':
                sum_price = sum_price + tick.close
        weight = (sum_price / len(ticks) / base_price) * rank.cap_ratio
        this.logger.debug('k10 calc - Calculated weight %s Is: %s for symbol: %s', price_field, weight, rank.symbol)
        return weight

    def calculate_symbol_volume(this, ticks):
        sum_volume = 0
        for tick in ticks:
            sum_volume = sum_volume + ((tick.high + tick.low) / 2 * tick.volume)
        this.logger.debug('k10 calc - Calculated Volume Is: %s for symbol: %s', sum_volume, ticks[0].symbol)
        return sum_volume

    def find_miss_price_market(this, symbols, ticks):
        if symbols is None:
            return None
        miss_market = []
        symbol_usdt = symbols[0]
        symbol_btc = symbols[1]
        index_usdt = this.symbols_all_market['default'].index(symbol_usdt)
        index_btc = this.symbols_all_market['default'].index(symbol_btc)
        valid_markets = this.find_all_valid_markets()
        if ticks == None:
            for key in valid_markets:
                if this.symbols_all_market[key][index_usdt] != '' or this.symbols_all_market[key][index_btc] != '':
                    miss_market.append(key)
        else:
            for key in valid_markets:
                market_exist = False
                for tick in ticks:
                    if tick.market == key:
                        market_exist = True
                        break
                if not market_exist:
                    if this.symbols_all_market[key][index_usdt] != '' or this.symbols_all_market[key][index_btc] != '':
                        miss_market.append(key)
        return miss_market

    def find_all_valid_markets(this):
        valid_markets = []
        symbol_dict = this.symbols_all_market
        for key in symbol_dict:
            if key != 'default' and key != '_title' and key != 'k10_daily_rank' and key != 'bittrex' and key != 'bitfinex' and key != 'bitstamp':
                valid_markets.append(key)
        return valid_markets

    def getSymbol(this, rank_symbol):
        if "BTC" == rank_symbol:
            return ['btcusdt', 'btcusdt']
        if "ETH" == rank_symbol:
            return ['ethusdt', 'ethbtc']
        if "XRP" == rank_symbol:
            return ['xrpusdt', 'xrpbtc']
        if "BCH" == rank_symbol:
            return ['bchusdt', 'bchbtc']
        if "EOS" == rank_symbol:
            return ['eosusdt', 'eosbtc']
        if "LTC" == rank_symbol:
            return ['ltcusdt', 'ltcbtc']
        if "ADA" == rank_symbol:
            return ['adausdt', 'adabtc']
        if "XLM" == rank_symbol:
            return ['xlmusdt', 'xlmbtc']
        if "MIOTA" == rank_symbol:
            return ['miotausdt', 'miotabtc']
        if "NEO" == rank_symbol:
            return ['neousdt', 'neobtc']
        if "XMR" == rank_symbol:
            return ['xmrusdt', 'xmrbtc']
        if "DASH" == rank_symbol:
            return ['dashusdt', 'dashbtc']
        if "XEM" == rank_symbol:
            return ['xemusdt', 'xembtc']
        if "TRX" == rank_symbol:
            return ['trxusdt', 'trxbtc']
        if "VEN" == rank_symbol:
            return ['venusdt', 'venbtc']
        if "ETC" == rank_symbol:
            return ['etcusdt', 'etcbtc']
        if "QTUM" == rank_symbol:
            return ['qtumusdt', 'qtumbtc']
        if "OMG" == rank_symbol:
            return ['omgusdt', 'omgbtc']
        if "BNB" == rank_symbol:
            return ['bnbusdt', 'bnbbtc']
        return None