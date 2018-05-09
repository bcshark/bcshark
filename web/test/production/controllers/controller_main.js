"use strict";
angular.module('MarketIndex', ['ngResource'])

.service('ConfigService', ConfigService)
.service('MarketService', MarketService)
.service('SymbolService', SymbolService)
.service('KlineService', KlineService)
.controller('TvKlineController', TvKlineController)
.controller('IndexTopCoinsController', IndexTopCoinsController);
