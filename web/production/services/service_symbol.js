var SymbolService = ['$resource', '$http', 'ConfigService', function($resource, $http, config_service) {
	var service = {};

    service.all = function(callback) {
        $resource(config_service.symbols).query({}, callback);
    };

    return service;
}];