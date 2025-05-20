"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _AnalyticsApp = require("./AnalyticsApp");

Object.keys(_AnalyticsApp).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _AnalyticsApp[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _AnalyticsApp[key];
    }
  });
});
//# sourceMappingURL=index.js.map