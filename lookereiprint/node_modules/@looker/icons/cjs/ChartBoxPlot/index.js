"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _ChartBoxPlot = require("./ChartBoxPlot");

Object.keys(_ChartBoxPlot).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _ChartBoxPlot[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _ChartBoxPlot[key];
    }
  });
});
//# sourceMappingURL=index.js.map