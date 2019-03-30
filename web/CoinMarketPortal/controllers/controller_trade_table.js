"use strict";

var TradeTableController = ['$scope', '$http', '$interval', function($scope, $http, $interval) {
	var myChart = echarts.init(document.getElementById('trade-in-chart'));

	$scope.isTradeInPanelOpened = true;

	$scope.trade_period_columns = [ {
		title: '时间',
		field: 'Time'
	}, {
		title: '买入',
		field: 'TradeIn'

	}, {
		title: '卖出',
		field: 'TradeOut'

	}, {
		title: '净流入',
		field: 'TradeInNet'

	} ];
	$scope.trade_period_rows = [];

	$scope.get_trade_period_row_value = function(trade_period_row, trade_period_column) {
		return null;
	}
}];
