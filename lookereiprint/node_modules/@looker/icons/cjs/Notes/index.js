"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _Notes = require("./Notes");

Object.keys(_Notes).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _Notes[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _Notes[key];
    }
  });
});
//# sourceMappingURL=index.js.map