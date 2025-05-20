"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _Public = require("./Public");

Object.keys(_Public).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _Public[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _Public[key];
    }
  });
});
//# sourceMappingURL=index.js.map