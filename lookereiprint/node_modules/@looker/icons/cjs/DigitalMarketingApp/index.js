"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _DigitalMarketingApp = require("./DigitalMarketingApp");

Object.keys(_DigitalMarketingApp).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _DigitalMarketingApp[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _DigitalMarketingApp[key];
    }
  });
});
//# sourceMappingURL=index.js.map