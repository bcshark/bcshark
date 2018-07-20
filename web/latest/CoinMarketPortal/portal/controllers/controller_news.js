"use strict";

var IndexNewsController = ['$scope', '$http', '$interval', '$location', '$window', function($scope, $http, $interval, $location, $window) {
	var DEFAULT_TOP_NEWS_COUNT = 50;

	$scope.getTopNews = function() {
		$http
			.get("https://news-headlines.tradingview.com/headlines/yahoo/symbol/?q=AAPL&locale=zh_CN") 
			.then(function(resp) {
				resp = resp.data;

				if (resp && resp instanceof Array) {
					$scope.top_news = resp;
				}
			}, function(resp) {
				if (!resp.data) {
					console.log('Cannot get top news.');
				}
			}
		);
	}

	$scope.getTopNews();
}];
