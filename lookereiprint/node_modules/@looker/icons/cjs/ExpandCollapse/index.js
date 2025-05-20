"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _ExpandCollapse = require("./ExpandCollapse");

Object.keys(_ExpandCollapse).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _ExpandCollapse[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _ExpandCollapse[key];
    }
  });
});
//# sourceMappingURL=index.js.map