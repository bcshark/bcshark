import requests
import json

DEFAULT_TIMEOUT_IN_SECONDS = 60
DEFAULT_OUTPUT_FILE = 'poloniex.matches.txt'
DEFAULT_WATCH_SYMBOLS = ['BTC', 'USDT', 'USD']

check_symbol_list = ['EOS', 'FIL', 'ADA', 'ZIL', 'ONT', 'ELF', 'AE', 'KNC', 'AGI', 'MANA', 'POWR', 'ENG', 'THETA', 'IOST', 'BLZ', 'ELA', 'BRD', 'ICX', 'DDD', 'ZRX', 'CVC', 'WAN', 'AION', 'DRGN', 'RDN', 'MOBI', 'RUFF', 'GNX', 'LRC', 'SNT', 'VEN', 'NANO', 'WTC', 'GNT', 'LOOM']

def http_request_json(url, headers = None, cookies = None):
    try:
        res = requests.get(url, headers = headers, cookies = cookies, timeout = DEFAULT_TIMEOUT_IN_SECONDS, allow_redirects = True)

        return res.json()
    except Exception, e:
        print e
        return None

if __name__ == '__main__':
	json_obj = http_request_json('https://poloniex.com/public?command=returnTicker')

	with open(DEFAULT_OUTPUT_FILE, 'wr') as out_file:
		for index in range(0, len(check_symbol_list)):
			symbol = check_symbol_list[index]
			ret = []

			if json_obj.has_key('BTC_%s' % symbol):
				ret.append('BTC')
			
			if json_obj.has_key('USDT_%s' % symbol):
				ret.append('USDT')

			if ret:
				out_file.write('%s\t%s\n' % (symbol, '/'.join(ret)))
			else:
				out_file.write('%s\t%s\n' % (symbol, 'NA'))
