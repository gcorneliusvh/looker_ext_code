"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _QuickStart = require("./QuickStart");

Object.keys(_QuickStart).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _QuickStart[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _QuickStart[key];
    }
  });
});
//# sourceMappingURL=index.js.map