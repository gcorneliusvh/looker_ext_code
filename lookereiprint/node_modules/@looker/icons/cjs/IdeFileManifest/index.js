"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _IdeFileManifest = require("./IdeFileManifest");

Object.keys(_IdeFileManifest).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _IdeFileManifest[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _IdeFileManifest[key];
    }
  });
});
//# sourceMappingURL=index.js.map