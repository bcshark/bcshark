var ConfigService = function($resource, $http) {
	var webapi_url = "http://192.168.56.101:5000"

	var config = {
		"markets": 				webapi_url + "/api/markets",
		"symbols": 				webapi_url + "/api/symbols",
		"market_index_kline": 	webapi_url + "/api/kline?m=:market&s=:symbol"
	};

	return config;
};