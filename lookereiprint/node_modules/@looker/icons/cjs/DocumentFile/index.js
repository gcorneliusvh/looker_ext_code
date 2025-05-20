"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _DocumentFile = require("./DocumentFile");

Object.keys(_DocumentFile).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _DocumentFile[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _DocumentFile[key];
    }
  });
});
//# sourceMappingURL=index.js.map