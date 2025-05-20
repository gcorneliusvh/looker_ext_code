"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _FieldTier = require("./FieldTier");

Object.keys(_FieldTier).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _FieldTier[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _FieldTier[key];
    }
  });
});
//# sourceMappingURL=index.js.map