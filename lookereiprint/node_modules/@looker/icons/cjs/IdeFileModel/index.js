"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _IdeFileModel = require("./IdeFileModel");

Object.keys(_IdeFileModel).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _IdeFileModel[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _IdeFileModel[key];
    }
  });
});
//# sourceMappingURL=index.js.map