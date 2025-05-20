"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _GitBranch = require("./GitBranch");

Object.keys(_GitBranch).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _GitBranch[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _GitBranch[key];
    }
  });
});
//# sourceMappingURL=index.js.map