angular.module('MarketIndex', ['ngRoute', 'cgBusy'])

.controller('MainController', function($scope, $route, $routeParams, $location) {
	$scope.$route = $route;
	$scope.$location = $location;
	$scope.$routeParams = $routeParams;
})
.config(function($routeProvider, $locationProvider) {
	$routeProvider
		.when('/', {
			templateUrl: 'views/kline.html',
			controller: KlineController,
		});
});
