"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _Reports = require("./Reports");

Object.keys(_Reports).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _Reports[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _Reports[key];
    }
  });
});
//# sourceMappingURL=index.js.map