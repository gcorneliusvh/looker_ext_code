"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.IdeFileDashboardDimensions = exports.IdeFileDashboard = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const IdeFileDashboard = _react.default.forwardRef((props, ref) => {
  const attrs = {
    "fill": "currentColor",
    "xmlns": "http://www.w3.org/2000/svg"
  };
  return _react.default.createElement(_styledIcon.StyledIconBase, _extends({
    iconAttrs: attrs,
    iconVerticalAlign: "middle",
    iconViewBox: "0 0 24 24"
  }, props, {
    ref: ref
  }), _react.default.createElement("path", {
    fillRule: "evenodd",
    clipRule: "evenodd",
    d: "M20 6H4V18H20V6ZM4 4C2.89543 4 2 4.89543 2 6V18C2 19.1046 2.89543 20 4 20H20C21.1046 20 22 19.1046 22 18V6C22 4.89543 21.1046 4 20 4H4Z"
  }), _react.default.createElement("path", {
    d: "M7 14C7 13.4477 7.44772 13 8 13C8.55228 13 9 13.4477 9 14V15C9 15.5523 8.55228 16 8 16C7.44772 16 7 15.5523 7 15V14Z"
  }), _react.default.createElement("path", {
    d: "M11 12C11 11.4477 11.4477 11 12 11C12.5523 11 13 11.4477 13 12V15C13 15.5523 12.5523 16 12 16C11.4477 16 11 15.5523 11 15V12Z"
  }), _react.default.createElement("path", {
    d: "M15 9C15 8.44772 15.4477 8 16 8C16.5523 8 17 8.44772 17 9V15C17 15.5523 16.5523 16 16 16C15.4477 16 15 15.5523 15 15V9Z"
  }));
});

exports.IdeFileDashboard = IdeFileDashboard;
IdeFileDashboard.displayName = 'IdeFileDashboard';
const IdeFileDashboardDimensions = {
  height: 24,
  width: 24
};
exports.IdeFileDashboardDimensions = IdeFileDashboardDimensions;
//# sourceMappingURL=IdeFileDashboard.js.map