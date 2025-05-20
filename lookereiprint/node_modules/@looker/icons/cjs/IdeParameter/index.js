"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _IdeParameter = require("./IdeParameter");

Object.keys(_IdeParameter).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _IdeParameter[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _IdeParameter[key];
    }
  });
});
//# sourceMappingURL=index.js.map