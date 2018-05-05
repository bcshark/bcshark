"use strict";

var TvKlineController = ['$scope', '$http', '$interval', '$window', 'MarketService', 'SymbolService', 'KlineService', function($scope, $http, $interval, $window, marketService, symbolService, klineService) {
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
		$scope.isNavCollapsed = true;
		getMarketTicks();
	};

	$scope.switchMarket = function(market) {
		$scope.selectedMarket = market;
		getMarketTicks();
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
			$scope.selectedSymbol = $scope.symbols[0].name;
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
		TradingView.onready(function() {
			var widget = window.tvWidget = new TradingView.widget({
				fullscreen: true,
				symbol: 'Index',
				interval: '1',
				container_id: "kline-chart",
				datafeed: new Datafeeds.UDFCompatibleDatafeed("http://192.168.56.101:5000/tv"),
				library_path: "/public/javascript/charting_library/",
				locale: getParameterByName('lang') || "en",
				drawings_access: { type: 'black', tools: [ { name: "Regression Trend" } ] },
				disabled_features: [ "study_templates" ],
				enabled_features: [ "use_localstorage_for_settings" ],
				charts_storage_url: 'http://192.168.56.101:5000/tv',
				charts_storage_api_version: "1.1",
				client_id: 'market_index',
				user_id: 'public_user_id'
			});
		});
	}

	getMarketTicks();

	/*
	getMarkets();
	getSymbols();

	$scope.$watch(function() {
		return $scope.isMarketsLoadded && $scope.isSymbolsLoadded;
	}, function(newValue, oldValue, scope) {
		if (newValue) {
			getMarketTicks();
		}
	});
	*/
}];
