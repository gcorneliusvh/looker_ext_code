"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _CalendarMonth = require("./CalendarMonth");

Object.keys(_CalendarMonth).forEach(function (key) {
  if (key === "default" || key === "__esModule") return;
  if (key in exports && exports[key] === _CalendarMonth[key]) return;
  Object.defineProperty(exports, key, {
    enumerable: true,
    get: function get() {
      return _CalendarMonth[key];
    }
  });
});
//# sourceMappingURL=index.js.map