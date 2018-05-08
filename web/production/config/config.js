var ConfigService = function($resource, $http) {
	var webapi_url = "http://18.218.165.228:5000"

	var config = {
		"markets": 				webapi_url + "/api/markets",
		"symbols": 				webapi_url + "/api/symbols",
		"market_index_kline": 	webapi_url + "/api/kline?m=:market&s=:symbol"
	};

	return config;
};
