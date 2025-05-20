"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _CrossFilter = require("./CrossFilter");

Object.keys(_CrossFilter).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _CrossFilter[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _CrossFilter[key];
    }
  });
});
//# sourceMappingURL=index.js.map