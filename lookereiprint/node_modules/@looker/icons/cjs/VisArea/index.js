"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _VisArea = require("./VisArea");

Object.keys(_VisArea).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _VisArea[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _VisArea[key];
    }
  });
});
//# sourceMappingURL=index.js.map