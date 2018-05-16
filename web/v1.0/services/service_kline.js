var KlineService = ['$resource', '$http', 'ConfigService', function($resource, $http, config_service) {
	var service = {};

	service.market_index_kline = function(market_name, symbol_name, callback) {
        $resource(config_service.market_index_kline).query({ market: market_name, symbol: symbol_name }, callback);
	};

	return service;
}];
