"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _SendWebhook = require("./SendWebhook");

Object.keys(_SendWebhook).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _SendWebhook[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _SendWebhook[key];
    }
  });
});
//# sourceMappingURL=index.js.map