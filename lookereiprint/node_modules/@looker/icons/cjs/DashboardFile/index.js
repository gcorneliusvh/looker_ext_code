"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _DashboardFile = require("./DashboardFile");

Object.keys(_DashboardFile).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _DashboardFile[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _DashboardFile[key];
    }
  });
});
//# sourceMappingURL=index.js.map