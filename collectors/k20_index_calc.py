import requests
import datetime

from model.market_tick import market_tick
from model.k20_rank import k20_rank
from model.k20_index import k20_index
from .collector import collector
from .utility import *

class collector_k20_index_calc(collector):
    DEFAULT_PERIOD = "1min"

    @property
    def market_name(this):
        return "k20_index_calc"

    def __init__(this, settings, market_settings):
        super(collector_k20_index_calc, this).__init__(settings, market_settings)

    def translate_ranks(this, objs):
        k20_ranks = []

        print('k20 calc - length of result rank: ', len(objs['series']))

        for obj in objs['series']:
            if len(obj['values']) < 1:
                print('K20 - Error! DB query result of Rank is 0: ', objs['values'])
                continue

            rank = k20_rank()
            rank.symbol = obj['values'][0][2]
            rank.time = long(obj['values'][0][1])
            rank.market_cap_usd = float(obj['values'][0][3])

            if rank.market_cap_usd <= 0:
                print('K20 - Error! Rank market cap is incorrect! ', rank.symbol, rank.market_cap_usd)
                continue
            k20_ranks.append(rank)

            print('generated rank: ', rank.time, rank.symbol, rank.market_cap_usd)
        print('length of generated rank: ', len(k20_ranks))

        return k20_ranks

    def translate_ticks(this, objs):
        ticks = []

        print('k20 calc - length of result ticks: ', len(objs['series']))

        for obj in objs['series']:

            if len(obj['values']) < 1:
                print('K20 - Error! DB query result of Tick is 0: ', objs["values"])
                continue

            tick = market_tick()
            tick.market = obj['values'][0][2]
            tick.symbol = obj['values'][0][3]
            tick.time = long(obj['values'][0][1])
            tick.high = float(obj['values'][0][4])
            tick.low = float(obj['values'][0][5])
            tick.open = float(obj['values'][0][6])
            tick.close = float(obj['values'][0][7])

            if tick.market == '' or tick.symbol == '':
                print('K20 - Error! Tick market/symbol is incorrect! ', tick.market, tick.symbol)
                continue

            ticks.append(tick)

            print('generated tick: ', tick.time, tick.market, tick.symbol, tick.high, tick.low, tick.open, tick.close)
        print('length of generated rick: ', len(ticks))

        return ticks

    def collect_rest(this):

        index = k20_index()
        rank_result = this.query_k20_daily_rank()
        #print('rank query return result:', rank_result)
        if len(rank_result) == 0:
            print('k20 - Error: table k20_daily_rank has no data, calculation failed, program exit')
            return
        #print('-------', len(rank_result['series']))
        #print('-------', rank_result['series'][0]['values'])
        ranks = this.translate_ranks(rank_result)
        if len(ranks) == 0:
            print('k20 - Error: translate k20 rank failed for: ', rank_result, ' program exit')
            return
        ranks = this.fillRatio(ranks)

        start_second = this.getStartSecondPreviousMinute()

        cal_length = len(ranks)
        print('k20 - rank size is: ', cal_length)
        total_high_weight = total_low_weight = total_open_weight = total_close_weight = 0

        for rank in ranks:

            tick_symbol = this.getSymbol(rank.symbol)
            print('Rank symbol related Tick symbol: ', tick_symbol)
            tick_result = this.query_latest_price(tick_symbol, start_second)
            #print('market_ticks return result:', tick_result)
            #print('market_ticks return result length:', len(tick_result))
            if len(tick_result) == 0:
                print('k20 - Warning: table market_ticks has no price for symbol: ', rank.symbol, ' bypass it!')
                cal_length = cal_length - 1
                continue
            ticks = this.translate_ticks(tick_result)
            if len(ticks) == 0:
                print('k20 - Warning: translate ticks failed for: ', tick_result, ' bypass it')
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
            print('K20 - Error: NONE symbol weight exist! program exit')
            return
        index.timezone_offset = -28800
        index.high = total_high_weight / cal_length
        index.low = total_low_weight / cal_length
        index.open = total_open_weight / cal_length
        index.close = total_close_weight / cal_length
        index.time = start_second
        index.period = '1min'

        print('index.timezone_offset:', index.timezone_offset)
        print('index.high:', index.high)
        print('index.low:', index.low)
        print('index.open:', index.open)
        print('index.close:', index.close)
        print('index.time:', index.time)
        print('index.period:', index.period)

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
        this.save_k20_index(indexArray[0])

        this.logger.info('K20 index calc done !')

    def getStartSecondPreviousMinute(this):
        time_second = int(time.time())
        time_second = time_second - time_second % 100 - 60
        print('start second generated: ', time_second)
        return time_second;

    def fillRatio(this, ranks):
        total_cap = 0
        for rank in ranks:
            total_cap = total_cap + rank.market_cap_usd
        for rank in ranks:
            rank.cap_ratio = rank.market_cap_usd / total_cap
            print('rank symbol: ', rank.symbol, ' Cap Ratio:', rank.cap_ratio)
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
        print('Calculated Avg Price ', price_field, ' Is: ', avg_price)
        return avg_price

    def getSymbol(this, rank_symbol):
        print('rank symbol mapping to tick symbol: ', rank_symbol)
        if "BTC" == rank_symbol:
            return "btcusdt"
        if "ETH" == rank_symbol:
            return "ethbtc"
        if "XRP" == rank_symbol:
            return "xrpbtc"
        if "BCH" == rank_symbol:
            return "bchbtc"
        if "EOS" == rank_symbol:
            return "eosbtc"
        if "LTC" == rank_symbol:
            return "ltcbtc"
        if "ADA" == rank_symbol:
            return "adabtc"
        if "XLM" == rank_symbol:
            return "xlmbtc"
        if "MIOTA" == rank_symbol:
            return "miotabtc"
        if "NEO" == rank_symbol:
            return "neobtc"
        if "XMR" == rank_symbol:
            return "xmrbtc"
        if "DASH" == rank_symbol:
            return "dashbtc"
        if "XEM" == rank_symbol:
            return "xembtc"
        if "TRX" == rank_symbol:
            return "trxbtc"
        if "VEN" == rank_symbol:
            return "venbtc"
        if "ETC" == rank_symbol:
            return "etcbtc"
        if "QTUM" == rank_symbol:
            return "qtumbtc"
        if "OMG" == rank_symbol:
            return "omgbtc"
        if "BNB" == rank_symbol:
            return "bnbbtc"
