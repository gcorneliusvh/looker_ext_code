"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _CalendarQuarter = require("./CalendarQuarter");

Object.keys(_CalendarQuarter).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _CalendarQuarter[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _CalendarQuarter[key];
    }
  });
});
//# sourceMappingURL=index.js.map