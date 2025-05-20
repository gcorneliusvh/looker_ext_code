"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _CalendarHour = require("./CalendarHour");

Object.keys(_CalendarHour).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _CalendarHour[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _CalendarHour[key];
    }
  });
});
//# sourceMappingURL=index.js.map