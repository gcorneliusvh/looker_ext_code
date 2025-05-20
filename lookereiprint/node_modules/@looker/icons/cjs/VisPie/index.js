"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _VisPie = require("./VisPie");

Object.keys(_VisPie).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _VisPie[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _VisPie[key];
    }
  });
});
//# sourceMappingURL=index.js.map