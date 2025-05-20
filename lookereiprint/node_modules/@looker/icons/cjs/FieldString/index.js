"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _FieldString = require("./FieldString");

Object.keys(_FieldString).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _FieldString[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _FieldString[key];
    }
  });
});
//# sourceMappingURL=index.js.map