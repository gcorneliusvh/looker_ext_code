"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _VisMap = require("./VisMap");

Object.keys(_VisMap).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _VisMap[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _VisMap[key];
    }
  });
});
//# sourceMappingURL=index.js.map