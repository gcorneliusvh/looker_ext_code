"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _IdeFileLookML = require("./IdeFileLookML");

Object.keys(_IdeFileLookML).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _IdeFileLookML[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _IdeFileLookML[key];
    }
  });
});
//# sourceMappingURL=index.js.map