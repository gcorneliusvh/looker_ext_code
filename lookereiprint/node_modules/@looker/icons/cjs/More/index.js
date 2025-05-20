"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _More = require("./More");

Object.keys(_More).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _More[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _More[key];
    }
  });
});
//# sourceMappingURL=index.js.map