"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _LookerLogo = require("./LookerLogo");

Object.keys(_LookerLogo).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _LookerLogo[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _LookerLogo[key];
    }
  });
});
//# sourceMappingURL=index.js.map