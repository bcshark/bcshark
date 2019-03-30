"use strict";

var MonitorController = ['$scope', '$http', '$interval', '$location', '$window', 'MonitorService', function($scope, $http, $interval, $location, $window, monitorService) {

	function getMonitors() {
		monitorService.monitor(
			function(resp) {
				if (resp && resp instanceof Array) {
                    $scope.setting_symbols = resp[0];
					$scope.monitors = resp[1];
					$scope.default_symbols = resp[0]["default"];
				}
			}
		);
	}

	$scope.filterMonitors = function (market, symbol) {
	    var match = false;
	    var result = "";
	    var i = 0;
        while($scope.monitors[i] != undefined) {
            var monitor = $scope.monitors[i];
            if (monitor.market == market && monitor.symbol == symbol) {
                var now = new Date();
                var offset_hour = now.getTimezoneOffset() / 60;
                var update_time = new Date(monitor.update_time);
                update_time.setHours(update_time.getHours() - offset_hour); //gmt to local time
                result = update_time.format("Y-M-d H:i:s");
                match = true;
                break;
            }
            i++;
        }
        if (!match) {
            var symbols = $scope.setting_symbols[market];
            var index = $scope.default_symbols.indexOf(symbol);
            if (symbols[index] != '') {
                result = "MISS";
            } else {
                result = "NA";
            }
        }
        return result;
    };

	$scope.setColor = function (date_str) {
	    if("" == date_str || null == date_str || undefined == date_str || "NA" == date_str) {
	        return {"background": 'black'};
	    } else if ("MISS" == date_str) {
	        return {"background": 'red'};
	    } else {
	        var date = new Date(date_str);
	        var diff_minute = (new Date() - date) / 1000 / 60;
	        if(diff_minute > 10) {
	            return {"background": 'red'};
	        } else {
                return {"background": 'black'};
             }
	    }
    };

	getMonitors();

}];
