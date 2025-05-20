"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _LQA = require("./LQA");

Object.keys(_LQA).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _LQA[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _LQA[key];
    }
  });
});
//# sourceMappingURL=index.js.map