import requests
import json

DEFAULT_TIMEOUT_IN_SECONDS = 60
DEFAULT_OUTPUT_FILE = 'huobi.matches.txt'
DEFAULT_WATCH_SYMBOLS = ['btc', 'usdt', 'usd']

check_symbol_list = ['EOS', 'IPFS', 'ADA', 'ZILLIQA', 'ONT', 'ELF', 'AE', 'KNC', 'AGI', 'MANA', 'POWR', 'ENG', 'THETA', 'IOST', 'BLZ', 'ELA', 'BRD', 'ICX', 'DDD', 'ZRX', 'CVC', 'WAN', 'AION', 'DRGN', 'RDN', 'MOBIUS', 'RUFF', 'GNX', 'LRC', 'SNT', 'VEN', 'NANO', 'WTC', 'GNT', 'LOOM']

def http_request_json(url, headers = None, cookies = None):
    try:
        res = requests.get(url, headers = headers, cookies = cookies, timeout = DEFAULT_TIMEOUT_IN_SECONDS, allow_redirects = True)

        return res.json()
    except Exception, e:
        print e
        return None

if __name__ == '__main__':
	json_obj = http_request_json('https://api.huobi.pro/v1/common/symbols')
	support_symbols = json_obj['data']

	with open(DEFAULT_OUTPUT_FILE, 'wr') as out_file:
		for index in range(0, len(check_symbol_list)):
			symbol = check_symbol_list[index]
			matches = filter(lambda x: x['base-currency'] == symbol.lower() and x['quote-currency'] in DEFAULT_WATCH_SYMBOLS, support_symbols)
			ret = '/'.join([match['quote-currency'].upper() for match in matches])
			if not ret:
				ret = 'NA'

			out_file.write('%s\t%s\n' % (symbol, ret))


