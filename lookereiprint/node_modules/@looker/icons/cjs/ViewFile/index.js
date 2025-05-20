"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _ViewFile = require("./ViewFile");

Object.keys(_ViewFile).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _ViewFile[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _ViewFile[key];
    }
  });
});
//# sourceMappingURL=index.js.map