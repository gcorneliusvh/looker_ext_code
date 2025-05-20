"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _IdeDimensionGroup = require("./IdeDimensionGroup");

Object.keys(_IdeDimensionGroup).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _IdeDimensionGroup[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _IdeDimensionGroup[key];
    }
  });
});
//# sourceMappingURL=index.js.map