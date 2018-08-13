var ConfigService = function($resource, $http) {
	var webapi_url = "http://18.218.165.228:5000"

	var config = {
		"markets": 				webapi_url + "/api/markets",
		"symbols": 				webapi_url + "/api/symbols",
		"market_index_kline": 	webapi_url + "/api/kline?m=:market&s=:symbol",
		"market_index_trend": 	webapi_url + "/api/trend?s=:symbol",
		"symbol_top_coins": 	webapi_url + "/api/topcoins?c=:count",
		"trading_view":			webapi_url + "/tv"

	};

	return config;
};
