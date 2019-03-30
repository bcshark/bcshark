"use strict";
angular.module('MarketIndex', ['ngResource'])

.service('ConfigService', ConfigService)
.service('MarketService', MarketService)
.service('SymbolService', SymbolService)
.service('KlineService', KlineService)
.service('MonitorService', MonitorService)
.service('ValidateService', ValidateService)
.controller('TvKlineController', TvKlineController)
.controller('TvKlineInnovationController', TvKlineInnovationController)
.controller('IndexNewsController', IndexNewsController)
.controller('IndexTopCoinsController', IndexTopCoinsController)
.controller('IndexTopCoinsTableController', IndexTopCoinsTableController)
.controller('MonitorController', MonitorController)
.controller('ValidateController', ValidateController);
