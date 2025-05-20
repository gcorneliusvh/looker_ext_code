"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _CheckProgress = require("./CheckProgress");

Object.keys(_CheckProgress).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _CheckProgress[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _CheckProgress[key];
    }
  });
});
//# sourceMappingURL=index.js.map