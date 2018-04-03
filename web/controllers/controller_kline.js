var KlineController = ['$scope', '$http', function($scope, $http) {
	$scope.promise = $http.get('http://192.168.56.101:5000/api/kline')
		.then(function(res) {
			console.log(res);
		});
}];
