"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _SalesAnalytics = require("./SalesAnalytics");

Object.keys(_SalesAnalytics).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _SalesAnalytics[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _SalesAnalytics[key];
    }
  });
});
//# sourceMappingURL=index.js.map