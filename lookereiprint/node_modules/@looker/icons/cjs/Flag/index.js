"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _Flag = require("./Flag");

Object.keys(_Flag).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _Flag[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _Flag[key];
    }
  });
});
//# sourceMappingURL=index.js.map