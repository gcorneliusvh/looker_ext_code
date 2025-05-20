"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _Download = require("./Download");

Object.keys(_Download).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _Download[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _Download[key];
    }
  });
});
//# sourceMappingURL=index.js.map