var MarketService = ['$resource', '$http', 'ConfigService', function($resource, $http, config_service) {
	var service = {};

	service.all = function(callback) {
        $resource(config_service.markets).query({}, callback);
	};

	return service;
}];
