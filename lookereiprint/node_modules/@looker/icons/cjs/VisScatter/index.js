"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _VisScatter = require("./VisScatter");

Object.keys(_VisScatter).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _VisScatter[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _VisScatter[key];
    }
  });
});
//# sourceMappingURL=index.js.map