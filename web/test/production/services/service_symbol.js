var SymbolService = ['$resource', '$http', 'ConfigService', function($resource, $http, config_service) {
	var service = {};

    service.all = function(callback) {
        $resource(config_service.symbols).query({}, callback);
    };

	service.top_coins = function(count, callback) {
        $resource(config_service.symbol_top_coins).query({ count: count }, callback);
	};

    return service;
}];
