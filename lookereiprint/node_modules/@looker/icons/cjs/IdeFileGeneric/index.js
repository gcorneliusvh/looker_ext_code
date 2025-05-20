"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _IdeFileGeneric = require("./IdeFileGeneric");

Object.keys(_IdeFileGeneric).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _IdeFileGeneric[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _IdeFileGeneric[key];
    }
  });
});
//# sourceMappingURL=index.js.map