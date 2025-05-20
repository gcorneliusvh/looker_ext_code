"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _VisSingleValue = require("./VisSingleValue");

Object.keys(_VisSingleValue).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _VisSingleValue[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _VisSingleValue[key];
    }
  });
});
//# sourceMappingURL=index.js.map