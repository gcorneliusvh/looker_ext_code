"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _DashboardGauge = require("./DashboardGauge");

Object.keys(_DashboardGauge).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _DashboardGauge[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _DashboardGauge[key];
    }
  });
});
//# sourceMappingURL=index.js.map