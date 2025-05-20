"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _VisLine = require("./VisLine");

Object.keys(_VisLine).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _VisLine[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _VisLine[key];
    }
  });
});
//# sourceMappingURL=index.js.map