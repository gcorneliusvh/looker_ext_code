"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _VisColumn = require("./VisColumn");

Object.keys(_VisColumn).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _VisColumn[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _VisColumn[key];
    }
  });
});
//# sourceMappingURL=index.js.map