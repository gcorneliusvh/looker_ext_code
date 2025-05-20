"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _VisTable = require("./VisTable");

Object.keys(_VisTable).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _VisTable[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _VisTable[key];
    }
  });
});
//# sourceMappingURL=index.js.map