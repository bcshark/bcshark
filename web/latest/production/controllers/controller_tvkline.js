"use strict";

var TvKlineController = ['$scope', '$http', '$interval', '$window', 'MarketService', 'SymbolService', 'KlineService', function($scope, $http, $interval, $window, marketService, symbolService, klineService) {
	var ENABLE_MOVING_AVERAGE = true;

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
				fullscreen: false,
				height: 500,
				width: '100%',
				symbol: 'Index',
				allow_symbol_change: true,
				symbol_search_request_delay: 1000,
				interval: '1',
				container_id: "kline-chart",
				datafeed: new Datafeeds.UDFCompatibleDatafeed("http://18.218.165.228:5000/tv"),
				library_path: "public/javascript/charting_library/",
				locale: getParameterByName('lang') || "en",
				drawings_access: { type: 'black', tools: [ { name: "Regression Trend" } ] },
				disabled_features: [ "study_templates", "left_toolbar", "control_bar", "timeframes_toolbar", "header_settings", "header_undo_redo", "header_interval_dialog_button", "header_screenshot", "header_saveload", "display_market_status", "use_localstorage_for_settings", "volume_force_overlay" ],
				// enabled_features: [ "use_localstorage_for_settings" ],
				charts_storage_url: 'http://18.218.165.228:5000/tv',
				charts_storage_api_version: "1.1",
				client_id: 'market_index',
				user_id: 'market_index',
				toolbar_bg: "#222222",
				overrides: {
					"volumePaneSize": "medium",
					"paneProperties.background": "#282828",
					"paneProperties.vertGridProperties.color": "#444243",
					"paneProperties.horzGridProperties.color": "#444243",
					"symbolWatermarkProperties.transparency": 90,
					"scalesProperties.textColor" : "#FFF"
				}
			});

			if (ENABLE_MOVING_AVERAGE) {
				widget.onChartReady(function() {
					widget.chart().createStudy('Moving Average', false, true, [  5, "close", 0 ], null, { 'plot.color': '#00CCFF' });
					widget.chart().createStudy('Moving Average', false, true, [ 10, "close", 0 ], null, { 'plot.color': '#66CC00' });
					widget.chart().createStudy('Moving Average', false, true, [ 30, "close", 0 ], null, { 'plot.color': '#CC99FF' });
					widget.chart().createStudy('Moving Average', false, true, [ 60, "close", 0 ], null, { 'plot.color': '#FF0066' });
				});
			}
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
