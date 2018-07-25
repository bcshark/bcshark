"use strict";

var KlineController = ['$scope', '$http', '$interval', '$location', '$window', 'MarketService', 'SymbolService', 'KlineService', function($scope, $http, $interval, $location, $window, marketService, symbolService, klineService) {
	var myChart = echarts.init(document.getElementById('kline-chart'));

	var upColor = '#ec0000';
	var upBorderColor = '#8A0000';
	var downColor = '#00da3c';
	var downBorderColor = '#008F28';
	var nextTickPromise = null;
	var isInitialized = false;

	$scope.isNavCollapsed = true;
	$scope.market_dropdown = {
		isopen : false,
		isdisabled : false
	};
	$scope.symbols = [];
	$scope.markets = [];
	$scope.selectedSymbol = null;
	$scope.selectedMarket = null;
	$scope.alerts = [];
	$scope.isMarketsLoadded = false;
	$scope.isSymbolsLoadded = false;

	$scope.switchSymbol = function(symbol) {
		$scope.selectedSymbol = symbol;
		getMarketTicks();
	};

	$scope.switchMarket = function(market) {
		$scope.selectedMarket = market;
		getMarketTicks();
	};

	$scope.showMarketIndex = function() {
		$scope.selectedMarket = { name: "market_index" , title: "Aggregate"};
		getMarketTicks();
	}

	$scope.showTradeView = function() {
		$location.path('/tvkline');
	};

	$scope.showMarketIndexView = function() {
		$location.path('/kline');
	};

	$scope.showTradeTable = function() {
		$location.path('/trade-table');
	};

	/*
	// 数据意义：开盘(open)，收盘(close)，最低(lowest)，最高(highest)
	var data0 = splitData([
		['2013/1/24', 2320.26,2320.26,2287.3,2362.94],
		['2013/1/25', 2300,2291.3,2288.26,2308.38],
		['2013/1/28', 2295.35,2346.5,2295.35,2346.92],
		['2013/6/4', 2297.1,2272.42,2264.76,2297.1],
		['2013/6/5', 2270.71,2270.93,2260.87,2276.86],
		['2013/6/6', 2264.43,2242.11,2240.07,2266.69],
		['2013/6/7', 2242.26,2210.9,2205.07,2250.63],
		['2013/6/13', 2190.1,2148.35,2126.22,2190.1]
	]);
	*/

	function splitData(rawData) {
		var categoryData = [];
		var values = [];

		for (var i = 0; i < rawData.length; i++) {
			categoryData.push(rawData[i]['0']);
			values.push([rawData[i]['1'], rawData[i]['2'], rawData[i]['3'], rawData[i]['4']]);
		}
		return {
			categoryData: categoryData,
			values: values
		};
	}

	function calculateAvg(data0) {
		var result = [];
		for (var i = 0, len = data0.length; i < len; i++) {
			result.push((data0[i][2] + data0[i][3]) / 2);
		}
		return result;
	}

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

	function showAlert(message) {
		$scope.alerts = [];
		$scope.alerts.push({ type: 'danger', msg: message });
	}
	
	$scope.closeAlert = function(index) {
		$scope.alerts.splice(index, 1);
	};

	function getMarkets() {
		marketService.all(function(resp) {
			$scope.markets = resp;
			$scope.selectedMarket = $scope.markets[0]
			$scope.isMarketsLoadded = true;
		});
	}

	function getSymbols() {
		symbolService.all(function(resp) {
			$scope.symbols = resp;
			$scope.selectedSymbol = $scope.symbols[0];
			$scope.isSymbolsLoadded = true;
		});
	}

	function getParameterByName(name) {
		name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
		var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
				results = regex.exec(location.search);
		return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
	}

	function getMarketTicks() {
		//$scope.promise = $http.get('http://192.168.56.101:5000/api/kline/' + $scope.selectedMarket.name + '/' + $scope.selectedSymbol)
		klineService.market_index_kline($scope.selectedMarket.name, $scope.selectedSymbol.name,
			function(resp) {
				var data0 = splitData(resp);
				var option = null;

				if (isInitialized) {
					option = {
						xAxis: { data: data0.categoryData },
						series: [ 
							{ data: data0.values },
							{ data: calculateAvg(data0.values) }
						]
					};

					myChart.setOption(option);				
				} else {
					option = {
						title: {
							text: $scope.selectedSymbol.name,
							left: 0
						},
						tooltip: {
							trigger: 'axis',
							axisPointer: {
								type: 'cross'
							}
						},
						legend: {
							bottom: 10,
							data: ['日K', 'Index']
						},
						grid: {
							left: 60,
							bottom: 100
							// left: '10%',
							// right: '10%',
							// bottom: '15%'
						},
						xAxis: {
							type: 'category',
							data: data0.categoryData,
							scale: true,
							boundaryGap : false,
							axisLine: {onZero: false},
							splitLine: {show: false},
							splitNumber: 20,
							min: 'dataMin',
							max: 'dataMax'
						},
						yAxis: {
							scale: true,
							splitArea: {
								show: true
							}
						},
						dataZoom: [{
								type: 'inside',
								start: 50,
								end: 100
							}, {
								show: true,
								type: 'slider',
								y: '90%',
								start: 50,
								end: 100,
								top: 410,
								bottom: 40
							}
						],
						series: [{
								name: '日K',
								type: 'candlestick',
								data: data0.values,
								itemStyle: {
									normal: {
										color: upColor,
										color0: downColor,
										borderColor: upBorderColor,
										borderColor0: downBorderColor
									}
								},
								markPoint: {
									label: {
										normal: {
											formatter: function (param) {
												return param != null ? Math.round(param.value) : '';
											}
										}
									},
									data: [{
											name: 'highest value',
											type: 'max',
											valueDim: 'highest'
										}, {
											name: 'lowest value',
											type: 'min',
											valueDim: 'lowest'
										}
									],
									tooltip: {
										formatter: function (param) {
											return param.name + '<br>' + (param.data.coord || '');
										}
									}
								}
							}, {
								name: 'Index',
								type: 'line',
								data: calculateAvg(data0.values),
								smooth: true,
								lineStyle: {
									normal: {opacity: 0.5}
								}
							}
						]
					}

					myChart.setOption(option);
					isInitialized = true;
				}
			}, function(resp) {
				if (!resp.data) {
					showAlert('Data is not provided by selected market.');
				}
			}
		);
	}

	getMarkets();
	getSymbols();

	$scope.$watch(function() {
		return $scope.isMarketsLoadded && $scope.isSymbolsLoadded;
	}, function(newValue, oldValue, scope) {
		if (newValue) {
			nextTickPromise = $interval(getMarketTicks, 20000, -1);
			getMarketTicks();
		}
	});
}];
