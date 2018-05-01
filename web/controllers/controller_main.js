angular.module('MarketIndex', ['ngRoute', 'ngResource', 'cgBusy', 'ui.bootstrap'])

.service('ConfigService', ConfigService)
.service('MarketService', MarketService)
.service('SymbolService', SymbolService)
.service('KlineService', KlineService)
.controller('MainController', function($scope, $route, $routeParams, $location) {
	$scope.$route = $route;
	$scope.$location = $location;
	$scope.$routeParams = $routeParams;
})
.config(function($routeProvider, $locationProvider) {
	$routeProvider
		.when('/', {
			templateUrl: 'views/index.html',
			controller: IndexController,
		}).when('/kline', {
			templateUrl: 'views/kline.html',
			controller: KlineController,
		}).when('/tvkline', {
			templateUrl: 'views/tvkline.html',
			controller: TvKlineController,
		}).when('/trade-table', {
			templateUrl: 'views/trade_table.html',
			controller: TradeTableController,
		});
});
