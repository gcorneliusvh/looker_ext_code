"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _Beaker = require("./Beaker");

Object.keys(_Beaker).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _Beaker[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _Beaker[key];
    }
  });
});
//# sourceMappingURL=index.js.map