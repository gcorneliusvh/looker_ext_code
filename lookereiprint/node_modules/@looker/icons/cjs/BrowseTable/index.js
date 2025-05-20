"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _BrowseTable = require("./BrowseTable");

Object.keys(_BrowseTable).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _BrowseTable[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _BrowseTable[key];
    }
  });
});
//# sourceMappingURL=index.js.map