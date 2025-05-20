"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _CalendarWeek = require("./CalendarWeek");

Object.keys(_CalendarWeek).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _CalendarWeek[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _CalendarWeek[key];
    }
  });
});
//# sourceMappingURL=index.js.map