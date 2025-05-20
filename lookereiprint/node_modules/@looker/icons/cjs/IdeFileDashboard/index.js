"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _IdeFileDashboard = require("./IdeFileDashboard");

Object.keys(_IdeFileDashboard).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _IdeFileDashboard[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _IdeFileDashboard[key];
    }
  });
});
//# sourceMappingURL=index.js.map