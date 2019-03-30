var MonitorService = ['$resource', '$http', 'ConfigService', function($resource, $http, config_service) {
	var service = {};

	service.monitor = function(callback) {
        $resource(config_service.monitor).query({}, callback);
	};

	return service;
}];