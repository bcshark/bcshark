"use strict";

var ValidateController = ['$scope', '$http', '$interval', '$location', '$window', 'ValidateService', function($scope, $http, $interval, $location, $window, validateService) {

    $scope.validate_market = "huobi";
    $scope.validate_start = new Date();
    $scope.validate_start.setHours($scope.validate_start.getHours()-1);
    $scope.validate_start.setSeconds(0);
    $scope.validate_start.setMilliseconds(0);
    $scope.validate_end = new Date();
    $scope.validate_end.setSeconds(0);
    $scope.validate_end.setMilliseconds(0);

	$scope.getvalidate = function() {
	    if($scope.validate_start == null || $scope.validate_start === undefined ) {
	        alert('Error! please enter start time');
	        return;
	    }
        if($scope.validate_end == null || $scope.validate_end === undefined ) {
            alert('Error! please enter end time');
            return;
        }
        var timezone_offset = new Date().getTimezoneOffset() * 60;
        var start_time = parseInt($scope.validate_start.getTime()/1000) + timezone_offset;
        var end_time = parseInt($scope.validate_end.getTime()/1000) + timezone_offset;
		validateService.validate(start_time, end_time, $scope.validate_market, $scope.validate_symbol,
			function(resp) {
				if (resp && resp instanceof Array) {
                    $scope.validate_count = resp;
				}
			}
		);
	};

    function fillMktSymbolValue() {
        validateService.fillMktSymbolValue(
            function(resp) {
                if (resp && resp instanceof Array) {
                    $scope.market_symbol_value = resp[0];
                    $scope.val_mkts = [];
                    var tmp_mkts = Object.keys(resp[0]);
                    for (var mkt in tmp_mkts) {
                        if (tmp_mkts[mkt] != '_title' && tmp_mkts[mkt] != 'k10_daily_rank' && tmp_mkts[mkt] != 'okex' && tmp_mkts[mkt] != 'bitfinex' && tmp_mkts[mkt] != 'default') {
                            $scope.val_mkts.push(tmp_mkts[mkt]);
                        }
                    }
                }
            }
        );
    }

    $scope.selectMarket = function() {
        var mkt = $scope.validate_market;
        var tmp_symbols = $scope.market_symbol_value[mkt];
        var symbl_array = [];
        for (var index in Object.keys(tmp_symbols)) {
            if (tmp_symbols[index] != "") {
                symbl_array.push($scope.market_symbol_value['default'][index]);
            }
        }
        $scope.val_syls = symbl_array;
    };

    fillMktSymbolValue();

}];
