"use strict";

var IndexController = ['$scope', '$http', '$interval', '$location', 
	function($scope, $http, $interval, $location) {
		$scope.openKline = function() {
	        $location.path('/kline');
		};

		$scope.openTradeTable = function() {
	        $location.path('/trade-table');
		};
	}
];
