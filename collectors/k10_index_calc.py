import requests
import datetime

from model.market_tick import market_tick
from model.k10_rank import k10_rank
from model.k10_index import k10_index
from .collector import collector
from .utility import *

class collector_k10_index_calc(collector):
    DEFAULT_PERIOD = "1min"

    @property
    def market_name(this):
        return "k10_index_calc"

    def __init__(this, settings, market_settings):
        super(collector_k10_index_calc, this).__init__(settings, market_settings)

    def translate_ranks(this, objs):
        this.logger.debug('rank result from db: %s', objs)
        k10_ranks = []
        for obj in objs:
            if int(obj[3]) > 10:
                continue
            rank = k10_rank()
            symbol = this.getSymbol(obj[1])
            this.logger.debug('k10 calc - get mapped Tick symbol: %s', symbol)
            if symbol == None:
                this.logger.debug('k10 calc - Warning: New symbol found on top 20, no price collected, bypass it!: %s', obj[1])
                continue
            rank.symbol = symbol
            rank.time = long(obj[0])
            rank.market_cap_usd = float(obj[2])
            rank.rank = int(obj[3])
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
            ticks.append(tick)
            this.logger.debug('k10 calc - Tick object generated from DB query: %s, %s, %s, %s, %s, %s, %s', tick.time, tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close)
        this.logger.debug('k10 calc - length of Tick object generated from DB query: %s', len(ticks))
        return ticks

    def collect_rest(this):

        start_second = this.getStartSecondPreviousMinute()
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
            this.logger.error('k10 calc Error - rank obj generated: %s not match result from DB query: %s, program exit', len(ranks), len(rank_result))
            return
        ranks = this.fillRatio(ranks)

        cal_length = len(ranks)
        this.logger.debug('k10 calc - length of rank object generated: %s', cal_length)
        total_high_weight = total_low_weight = total_open_weight = total_close_weight = 0

        for rank in ranks:
            print(rank.symbol[0], rank.symbol[1], start_second)
            tick_result = this.query_previous_min_price(rank.symbol[0], rank.symbol[1], start_second)
            if tick_result == None:
                #remove this rank from array and re-calculate the ratio for all ranks
                #rank_index = ranks.index(rank)
                #del ranks[rank_index]
                #ranks = this.fillRatio(ranks)
                tick_result_exist = this.query_latest_price_exist(rank.symbol[0], rank.symbol[1])
                if tick_result_exist == None:
                    continue
                tick_result = tick_result_exist
            ticks = this.translate_ticks(tick_result)
            avg_high = this.calculateSymbolAvgPrice(ticks, 'high')
            total_high_weight = total_high_weight + avg_high * rank.cap_ratio
            avg_low = this.calculateSymbolAvgPrice(ticks, 'low')
            total_low_weight = total_low_weight + avg_low * rank.cap_ratio
            avg_open = this.calculateSymbolAvgPrice(ticks, 'open')
            total_open_weight = total_open_weight + avg_open * rank.cap_ratio
            avg_close = this.calculateSymbolAvgPrice(ticks, 'close')
            total_close_weight = total_close_weight + avg_close * rank.cap_ratio

        if cal_length == 0:
            this.logger.error('k10 calc Error - validated symbol weight is 0 ! program exit')
            return
        index.timezone_offset = this.timezone_offset
        index.high = (total_high_weight / cal_length) * 100
        index.low = (total_low_weight / cal_length) * 100
        index.open = (total_open_weight / cal_length) * 100
        index.close = (total_close_weight / cal_length) * 100
        index.time = start_second
        index.period = '1min'

        this.logger.debug('k10 calc - index.timezone_offset: %s', index.timezone_offset)
        this.logger.debug('k10 calc - index.high: %s', index.high)
        this.logger.debug('k10 calc - index.low: %s', index.low)
        this.logger.debug('k10 calc - index.open: %s', index.open)
        this.logger.debug('k10 calc - index.close: %s', index.close)
        this.logger.debug('k10 calc - index.time: %s', index.time)
        this.logger.debug('k10 calc - index.period: %s', index.period)

        indexObj = {
            "time": index.time,
            "timezone_offset": index.timezone_offset,
            "open": index.open,
            "close": index.close,
            "low": index.low,
            "high": index.high,
            "period": index.period
        }
        indexArray = [indexObj]
        if index.high != 0 and index.low != 0 and index.open != 0 and index.close != 0:
            this.save_k10_index(indexArray[0])

        this.logger.info('k10 index calc done !')

    def getStartSecondPreviousMinute(this):
        time_second = int(time.time())
        time_second = time_second - (time_second % 60) - 120 + this.timezone_offset
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


    def calculateSymbolAvgPrice(this, ticks, price_field):

        sum_price = 0
        for tick in ticks:
            if price_field == 'high':
                sum_price = sum_price + tick.high
            if price_field == 'low':
                sum_price = sum_price + tick.low
            if price_field == 'open':
                sum_price = sum_price + tick.open
            if price_field == 'close':
                sum_price = sum_price + tick.close

        avg_price = sum_price/(len(ticks))
        this.logger.debug('k10 calc - Calculated Avg Price %s Is: %s for symbol: %s', price_field, avg_price, ticks[0].symbol)
        return avg_price

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