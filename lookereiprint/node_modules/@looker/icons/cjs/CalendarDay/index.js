"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _CalendarDay = require("./CalendarDay");

Object.keys(_CalendarDay).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _CalendarDay[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _CalendarDay[key];
    }
  });
});
//# sourceMappingURL=index.js.map