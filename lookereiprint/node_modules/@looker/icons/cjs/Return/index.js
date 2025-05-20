"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _Return = require("./Return");

Object.keys(_Return).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _Return[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _Return[key];
    }
  });
});
//# sourceMappingURL=index.js.map