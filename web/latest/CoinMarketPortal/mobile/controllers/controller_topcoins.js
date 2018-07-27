"use strict";

var IndexTopCoinsController = ['$scope', '$http', '$interval', '$location', '$window', 'MarketService', 'SymbolService', 'KlineService', function($scope, $http, $interval, $location, $window, marketService, symbolService, klineService) {
	var DEFAULT_TOP_COINS_COUNT = 4;
	var SHOW_SYMBOL_CHART = true;

	function getTopCoinChart(coin) {
		if (coin && coin.symbol) {
			klineService.market_index_trend(coin.symbol, function(resp) {
				var echartLine = echarts.init(document.getElementById('coin_chart_' + coin.symbol), 'dark');

				var date_text = [];
				var price_value = [];
				var max_value = resp[0]["1"];
				var min_value = resp[0]["1"];

				for (var index = 0; index < 5; index++) {
					var price = resp[index]["1"];

					date_text.push(resp[index]["0"]);
					price_value.push(price);

					if (price > max_value) max_value = price; 
					if (price < min_value) min_value = price; 
				}

				echartLine.setOption({
					title: {
					  show: false,
					  text: 'Symbol Price',
					  subtext: 'Symbol Price'
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
					  type: 'value',
					  min: Math.floor(min_value * 0.9),
					  max: Math.floor(max_value * 1.1)
					}],
					series: [{
					  name: 'Price',
					  type: 'line',
					  smooth: true,
					  itemStyle: {
						normal: { }
					  },
					  data: price_value
					}]
				  });
			});
		}
	}

	function getTopCoins(count) {
		symbolService.top_coins(count,
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
	
	getTopCoins(DEFAULT_TOP_COINS_COUNT);
}];
