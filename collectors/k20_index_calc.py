import requests
import datetime

from model.market_tick import market_tick
from model.k20_rank import k20_rank
from model.k20_index import k20_index
from .collector import collector
from .utility import *

class collector_k20_index_calc(collector):
    DEFAULT_PERIOD = "1min"

    def __init__(this, settings, market_settings):
        super(collector_k20_index_calc, this).__init__(settings, market_settings)

    def translate_ranks(this, objs):
        k20_ranks = []

        print('k20 calc - length of result rank: ', len(objs['series']))

        for obj in objs['series']:

            if len(objs["value"]) < 1:
                print('K20 - Error! DB query result of Rank is 0: ', objs["values"])
                continue

            rank = k20_rank()
            rank.symbol = obj["values"][0][2]
            rank.time = long(obj["values"][0][1])
            rank.market_cap_usd = float(obj["values"][0][3])

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

            if len(objs["value"]) < 1:
                print('K20 - Error! DB query result of Tick is 0: ', objs["values"])
                continue

            tick = market_tick()
            tick.market = obj["values"][0][2]
            tick.symbol = obj["values"][0][3]
            tick.time = long(obj["values"][0][1])
            tick.high = float(obj["values"][0][4])
            tick.low = float(obj["values"][0][5])
            tick.open = float(obj["values"][0][6])
            tick.close = float(obj["values"][0][7])

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
        #print('market_ticks return result:', rank_result)
        #print('-------', len(rank_result['series']))
        #print('-------', rank_result['series'][0]['values'])
        ranks = this.translate_ranks(rank_result)
        ranks = this.fillRatio(ranks)

        start_second = this.getStartSecondPreviousMinute()

        total_high_weight = total_low_weight = total_open_weight = total_close_weight = 0

        for rank in ranks:

            tick_symbol = this.getSymbol(rank.symbol)
            print('Rank symbol related Tick symbol: ', tick_symbol)
            tick_result = this.query_latest_price(tick_symbol, start_second)
            #print('market_ticks return result:', tick_result)
            ticks = this.translate_ticks(tick_result)
            avg_high = this.calculateSymbolAvgPrice(ticks, 'high')
            total_high_weight = total_high_weight + avg_high * rank.cap_ratio
            avg_low = this.calculateSymbolAvgPrice(ticks, 'low')
            total_low_weight = total_low_weight + avg_low * rank.cap_ratio
            avg_open = this.calculateSymbolAvgPrice(ticks, 'open')
            total_open_weight = total_open_weight + avg_open * rank.cap_ratio
            avg_close = this.calculateSymbolAvgPrice(ticks, 'close')
            total_close_weight = total_close_weight + avg_close * rank.cap_ratio

        index.timezone_offset = -28800
        index.high = total_high_weight / len(ranks)
        index.low = total_low_weight / len(ranks)
        index.open = total_open_weight / len(ranks)
        index.close = total_close_weight / len(ranks)
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

    def calculateSymbolAvgPriceExcludeHighLow(this, ticks, price_field):

        print('input price field for avg cal: ', price_field)
        print('ticks length: ', len(ticks), ticks[0].__dict__)
        if len(ticks) == 0:
            this.logger.error('total size of ticks is 0 !')
            return 0
        elif len(ticks) == 2:
            this.logger.warn('total size of ticks is:', len(ticks))
            this.logger.warn('limited exchange price for symbol, index calculation may wave drastic')
            return (float(ticks[0].__dict__[price_field]) + float(ticks[1].__dict__[price_field])) / 2
        else:
            this.logger.info('total size of ticks is:', len(ticks))

            high = low = float(ticks[0].__dict__[price_field])
            print('converted high = low = ', high)

            high_market = low_market = ticks[0].market
            for tick in ticks:
                field_value = float(tick.__dict__[price_field])
                print('in ticks loop converted filed value: ', field_value)
                if field_value > high:
                    high = field_value
                    high_market = tick.market
                if field_value < low:
                    low = field_value
                    low_market = tick.market

            print('high market: ', high_market, ' high price: ', high, 'low market: ', low_market, ' low price: ', low)

            total = 0
            for tick in ticks:
                if tick.market == high_market or tick.market == low_market:
                    print('discard market: ', tick.market)
                else:
                    total = float(tick.__dict__[price_field]) + total
                    print('total: ', total)
            return total/(len(ticks) - 2)

    def calculateSymbolAvgPrice(this, ticks, price_field):

        if len(ticks) == 0:
            this.logger.error('K20 calc Error! No price record found for such symbol! ')
            return 0
        else:
            this.logger.info('Total size of ticks is:', len(ticks))

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
            print('Calculated Avg Price Is: ', avg_price)
            return avg_price

    def getSymbol(this, rank_symbol):
        print('rank symbol mapping to tick symbol: ', rank_symbol)
        if "BTC" == rank_symbol:
            return "btcusdt"
        if "ETH" == rank_symbol:
            return "ethusdt"
        if "XRP" == rank_symbol:
            return "xrpusdt"
        if "BCH" == rank_symbol:
            return "bchusdt"
        if "EOS" == rank_symbol:
            return "eosusdt"
        if "LTC" == rank_symbol:
            return "ltcusdt"
        if "ADA" == rank_symbol:
            return "adausdt"
        if "XLM" == rank_symbol:
            return "xlmusdt"
        if "MIOTA" == rank_symbol:
            return "miotausdt"
        if "NEO" == rank_symbol:
            return "neousdt"
        if "XMR" == rank_symbol:
            return "xmrusdt"
        if "DASH" == rank_symbol:
            return "dashusdt"
        if "XEM" == rank_symbol:
            return "xemusdt"
        if "TRX" == rank_symbol:
            return "trxusdt"
        if "VEN" == rank_symbol:
            return "venusdt"
        if "ETC" == rank_symbol:
            return "etcusdt"
        if "QTUM" == rank_symbol:
            return "qtumusdt"
        if "OMG" == rank_symbol:
            return "omgusdt"
        if "BNB" == rank_symbol:
            return "bnbusdt"
