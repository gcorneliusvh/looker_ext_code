"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _IdeDimension = require("./IdeDimension");

Object.keys(_IdeDimension).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _IdeDimension[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _IdeDimension[key];
    }
  });
});
//# sourceMappingURL=index.js.map