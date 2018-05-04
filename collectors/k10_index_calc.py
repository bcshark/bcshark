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
        k10_ranks = []

        # this.logger.debug('k10 calc info - k10_daily_rank query return result length: %s', len(objs['series']))
        for obj in objs:
            # if len(obj['values']) < 1:
            #     this.logger.error('k10 cac Error - k10_daily_rank query return result length 0: %s', objs['values'])
            #     continue

            rank = k10_rank()
            rank.symbol = obj[1]
            rank.time = long(obj[0])
            rank.market_cap_usd = float(obj[2])

            if rank.market_cap_usd <= 0:
                this.logger.error('k10 calc Error - k10_daily_rank query result market cap is incorrect! %s, %s', rank.symbol, rank.market_cap_usd)
                continue
            k10_ranks.append(rank)

            this.logger.debug('k10 calc - rank object generated from DB query: %s, %s, %s', rank.time, rank.symbol, rank.market_cap_usd)
        this.logger.debug('k10 calc - length of rank object generated from DB query: %s', len(k10_ranks))

        return k10_ranks

    def translate_ticks(this, objs):
        ticks = []

        this.logger.debug('k10 calc - market_ticks query return result length: %s', len(objs['series']))

        for obj in objs['series']:

            if len(obj['values']) < 1:
                this.logger.error('k10 calc Error - market_ticks query return result length 0: %s', objs["values"])
                continue

            tick = market_tick()
            tick.market = obj['tags']['market']
            tick.symbol = obj['tags']['symbol']
            tick.time = long(obj['values'][0][0])
            tick.high = float(obj['values'][0][1])
            tick.low = float(obj['values'][0][2])
            tick.open = float(obj['values'][0][3])
            tick.close = float(obj['values'][0][4])

            if tick.market == '' or tick.symbol == '':
                this.logger.error('k10 calc Error - market_ticks query result Market/Symbol is incorrect! %s, %s', tick.market, tick.symbol)
                continue

            ticks.append(tick)

            this.logger.debug('k10 calc - Tick object generated from DB query: %s, %s, %s, %s, %s, %s, %s', tick.time, tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close)
        this.logger.debug('k10 calc - length of Tick object generated from DB query: %s', len(ticks))

        return ticks

    def collect_rest(this):

        index = k10_index()
        ### this should be limited to latest 5 mins !!!!
        rank_result = this.query_k10_daily_rank()
        print('rank query return result:', rank_result)
        # if len(rank_result) == 0:
        #     this.logger.error('k10 calc Error - k10_daily_rank table has no data, calculation failed, program exit')
        #     return
        #print('-------', len(rank_result['series']))
        #print('-------', rank_result['series'][0]['values'])
        ranks = this.translate_ranks(rank_result)
        if len(ranks) == 0:
            this.logger.error('k10 calc Error - translate k10 rank object failed for: %s, program exit', rank_result)
            return
        ranks = this.fillRatio(ranks)

        start_second = this.getStartSecondPreviousMinute()

        cal_length = len(ranks)
        this.logger.debug('k10 calc - length of rank object generated: %s', cal_length)
        total_high_weight = total_low_weight = total_open_weight = total_close_weight = 0

        for rank in ranks:

            tick_symbol = this.getSymbol(rank.symbol)
            this.logger.debug('k10 calc - get mapping Tick symbol: %s', tick_symbol)
            if tick_symbol == None:
                this.logger.debug('k10 calc - Warning: New symbol %s  found on top 20, no price collected, bypass it!', rank.symbol)
                continue
            tick_result = this.query_latest_price(tick_symbol[0], tick_symbol[1], start_second)
            #print('market_ticks return result:', tick_result)
            #print('market_ticks return result:', tick_result['series'][0]['values'][0][1])
            if len(tick_result) == 0 or not tick_result.has_key('series') or tick_result['series'][0]['values'][0][1] == None:
                this.logger.warn('k10 calc - Warning: market_ticks table has no price for symbol: %s  bypass it!', tick_symbol)
                cal_length = cal_length - 1
                continue
            ticks = this.translate_ticks(tick_result)
            if len(ticks) == 0:
                this.logger.warn('k10 calc - Warning: translate ticks failed for: %s  bypass it', tick_result)
                cal_length = cal_length - 1
                continue
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
        if index.high != 0:
            this.save_k10_index(indexArray[0])

        this.logger.info('k10 index calc done !')

    def getStartSecondPreviousMinute(this):
        time_second = int(time.time())
        time_second = time_second - (time_second % 60) - 60 + this.timezone_offset
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
