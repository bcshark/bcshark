"use strict";

var IndexController = ['$scope', '$http', '$interval', '$location', 
	function($scope, $http, $interval, $location) {
		$scope.openTvKline = function() {
	        $location.path('/tvkline');
		};

		$scope.openKline = function() {
	        $location.path('/kline');
		};

		$scope.openTradeTable = function() {
	        $location.path('/trade-table');
		};
	}
];
