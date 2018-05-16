"use strict";

var IndexTopCoinsTableController = ['$scope', '$http', '$interval', '$location', '$window', 'MarketService', 'SymbolService', 'KlineService', function($scope, $http, $interval, $location, $window, marketService, symbolService, klineService) {
	var DEFAULT_TOP_COINS_COUNT = 10;
	var SHOW_SYMBOL_CHART = false;

	function getTopCoinChart(coin) {
		if (coin && coin.symbol) {
			klineService.market_index_trend(coin.symbol, function(resp) {
				var echartLine = echarts.init(document.getElementById('coin_chart_' + coin.symbol));

				var date_text = [];
				var price_value = [];

				for (var index = 0; index < 5; index++) {
					date_text.push(resp[index]["0"]);
					price_value.push(resp[index]["1"]);
				}

				echartLine.setOption({
					title: {
					  show: false,
					  text: 'Line Graph',
					  subtext: 'Subtitle'
					},
					grid: {
						top: 20,
						left: 40,
						right: 20,
						bottom: 20,
					},
					tooltip: {
					  trigger: 'axis',
					},
					legend: {
					  show: false,
					  data: ['Intent']
					},
					toolbox: {
					  show: false,
					},
					calculable: true,
					xAxis: [{
					  type: 'category',
					  boundaryGap: false,
					  data: date_text
					}],
					yAxis: [{
					  type: 'value'
					}],
					series: [{
					  name: 'Price',
					  type: 'line',
					  smooth: true,
					  itemStyle: {
						normal: {
						  areaStyle: {
							type: 'default'
						  }
						}
					  },
					  data: price_value
					}]
				  });
			});
		}
	}

	$scope.getTopCoins = function() {
		symbolService.top_coins(DEFAULT_TOP_COINS_COUNT,
			function(resp) {
				if (resp && resp instanceof Array) {
					$scope.top_coins = resp;

					if (SHOW_SYMBOL_CHART) {
						for (var index = 0; index < resp.length; index++) {
							getTopCoinChart(resp[index]);
						}
					}
				}
			}, function(resp) {
				if (!resp.data) {
					console.log('Cannot get top coins.');
				}
			}
		);
	}

	//getMarkets();
	//getSymbols();
	
	$scope.getTopCoins();
}];
