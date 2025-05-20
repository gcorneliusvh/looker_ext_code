"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _ModelFile = require("./ModelFile");

Object.keys(_ModelFile).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _ModelFile[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _ModelFile[key];
    }
  });
});
//# sourceMappingURL=index.js.map