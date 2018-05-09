"use strict";

var IndexTopCoinsController = ['$scope', '$http', '$interval', '$location', '$window', 'MarketService', 'SymbolService', 'KlineService', function($scope, $http, $interval, $location, $window, marketService, symbolService, klineService) {
	var DEFAULT_TOP_COINS_COUNT = 4;

	function calculateMA(data0, dayCount) {
		var result = [];
		for (var i = 0, len = data0.values.length; i < len; i++) {
			if (i < dayCount) {
				result.push('-');
				continue;
			}
			var sum = 0;
			for (var j = 0; j < dayCount; j++) {
				sum += data0.values[i - j][1];
			}
			result.push(sum / dayCount);
		}
		return result;
	}

	function getTopCoinChart(coin) {
		if (coin && coin.symbol) {
			klineService.market_index_trend(coin.symbol, function(resp) {
				console.log(resp);
				var echartLine = echarts.init(document.getElementById('coin_chart_' + coin.symbol));

				echartLine.setOption({
					title: {
					  show: false,
					  text: 'Line Graph',
					  subtext: 'Subtitle'
					},
					grid: {
						top: 0,
						left: 0,
						right: 0,
						bottom: 0,
					},
					tooltip: {
					  trigger: 'axis',
					  formatter: function(value) {
						  return value;
					  }
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
					  data: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
					}],
					yAxis: [{
					  type: 'value'
					}],
					series: [{
					  name: 'Intent',
					  type: 'line',
					  smooth: true,
					  itemStyle: {
						normal: {
						  areaStyle: {
							type: 'default'
						  }
						}
					  },
					  data: [1320, 1132, 601, 234, 120, 90, 20]
					}]
				  });
			});
		}
	}

	function getTopCoins() {
		symbolService.top_coins(DEFAULT_TOP_COINS_COUNT,
			function(resp) {
				if (resp && resp instanceof Array) {
					$scope.top_coins = resp;

					for (var index = 0; index < resp.length; index++) {
						var init_chart = function(coin) {
							$interval(function() { 
								getTopCoinChart(coin); 
							}, 200, 1)
						};

						init_chart(resp[index]);
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
	
	getTopCoins();
}];
