var KlineController = ['$scope', '$http', '$interval', function($scope, $http, $interval) {
	var myChart = echarts.init(document.getElementById('kline-chart'));

	var upColor = '#ec0000';
	var upBorderColor = '#8A0000';
	var downColor = '#00da3c';
	var downBorderColor = '#008F28';
	var nextTickPromise = null;

	$scope.isNavCollapsed = true;
	$scope.market_dropdown = {
		isopen : false,
		isdisabled : false
	};
	$scope.symbols = [
		{ title : 'BTC - USDT', name : 'btcusdt' },
		{ title : 'ETH - BTC', name : 'ethbtc' },
		{ title : 'ETC - BTC', name : 'etcbtc' },
		{ title : 'EOS - BTC', name : 'eosbtc' }
	];
	$scope.markets = [
		{ title : 'Huobi', name : 'huobi' },
		{ title : 'Binance', name : 'binance' },
		{ title : 'OKEX', name : 'okex' },
		{ title : 'Poloniex', name : 'poloniex' },
		{ title : 'OkCoin', name : 'okcoin' },
		{ title : 'GDAX', name : 'gdax' },
		{ title : 'Bitfinex', name : 'bitfinex' },
		{ title : 'Bitstamp', name : 'bitstamp' },
		{ title : 'Bittrex', name : 'bittrex' }
	];
	$scope.selectedSymbol = 'btcusdt';
	$scope.selectedMarket = $scope.markets[0]
	$scope.alerts = [];

	$scope.switchSymbol = function(symbol) {
		$scope.selectedSymbol = symbol;
		$scope.isNavCollapsed = true;
		getMarketTicks();
	}

	$scope.switchMarket = function(market) {
		$scope.selectedMarket = market;
		getMarketTicks();
	}

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
		var values = []
		for (var i = 0; i < rawData.length; i++) {
			categoryData.push(rawData[i].splice(0, 1)[0]);
			values.push(rawData[i])
		}
		return {
			categoryData: categoryData,
			values: values
		};
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

	var isInitialized = false;

	function getMarketTicks() {
		//$scope.promise = $http.get('http://192.168.56.101:5000/api/kline/' + $scope.selectedMarket.name + '/' + $scope.selectedSymbol)
		$http.get('http://192.168.56.101:5000/api/kline/' + $scope.selectedMarket.name + '/' + $scope.selectedSymbol)
			.then(function(resp) {
				data0 = splitData(resp.data);

				if (isInitialized) {
					option = {
						xAxis: { data: data0.categoryData },
						series: [ 
							{ data: data0.values },
							{ data: calculateMA(data0, 5) },
							{ data: calculateMA(data0, 10) },
							{ data: calculateMA(data0, 20) },
							{ data: calculateMA(data0, 30) } 
						]
					};

					myChart.setOption(option);				
				} else {
					option = {
						title: {
							text: $scope.selectedSymbol,
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
							data: ['日K', 'MA5', 'MA10', 'MA20', 'MA30']
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
											name: 'XX标点',
											coord: ['2013/5/31', 2300],
											value: 2300,
											itemStyle: {
												normal: {color: 'rgb(41,60,85)'}
											}
										}, {
											name: 'highest value',
											type: 'max',
											valueDim: 'highest'
										}, {
											name: 'lowest value',
											type: 'min',
											valueDim: 'lowest'
										}, {
											name: 'average value on close',
											type: 'average',
											valueDim: 'close'
										}
									],
									tooltip: {
										formatter: function (param) {
											return param.name + '<br>' + (param.data.coord || '');
										}
									}
								},
								markLine: {
									symbol: ['none', 'none'],
									data: [
										[{
												name: 'from lowest to highest',
												type: 'min',
												valueDim: 'lowest',
												symbol: 'circle',
												symbolSize: 10,
												label: {
													normal: {show: false},
													emphasis: {show: false}
												}
											}, {
												type: 'max',
												valueDim: 'highest',
												symbol: 'circle',
												symbolSize: 10,
												label: {
													normal: {show: false},
													emphasis: {show: false}
												}
											}
										], {
											name: 'min line on close',
											type: 'min',
											valueDim: 'close'
										}, {
											name: 'max line on close',
											type: 'max',
											valueDim: 'close'
										}
									]
								}
							}, {
								name: 'MA5',
								type: 'line',
								data: calculateMA(data0, 5),
								smooth: true,
								lineStyle: {
									normal: {opacity: 0.5}
								}
							}, {
								name: 'MA10',
								type: 'line',
								data: calculateMA(data0, 10),
								smooth: true,
								lineStyle: {
									normal: {opacity: 0.5}
								}
							}, {
								name: 'MA20',
								type: 'line',
								data: calculateMA(data0, 20),
								smooth: true,
								lineStyle: {
									normal: {opacity: 0.5}
								}
							}, {
								name: 'MA30',
								type: 'line',
								data: calculateMA(data0, 30),
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

	nextTickPromise = $interval(getMarketTicks, 20000, -1);
	getMarketTicks();
}];
