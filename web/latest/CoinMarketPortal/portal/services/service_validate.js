var ValidateService = ['$resource', '$http', 'ConfigService', function($resource, $http, config_service) {
	var service = {};

    service.validate = function(start_time, end_time, market, symbol, callback) {
        $resource(config_service.validate).query({ start_time : start_time, end_time : end_time, market : market, symbol : symbol }, callback);
    };

    service.fillMktSymbolValue = function(callback) {
        $resource(config_service.fillMktSymbolValue).query({}, callback);
    };

	return service;
}];