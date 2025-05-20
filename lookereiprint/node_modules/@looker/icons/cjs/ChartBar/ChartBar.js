"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ChartBarDimensions = exports.ChartBar = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const ChartBar = _react.default.forwardRef((props, ref) => {
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
    d: "M14.8 5V8L5 8V5L14.8 5ZM19 10.5V13.5H5V10.5H19ZM11 16V19H5V16H11Z"
  }));
});

exports.ChartBar = ChartBar;
ChartBar.displayName = 'ChartBar';
const ChartBarDimensions = {
  height: 24,
  width: 24
};
exports.ChartBarDimensions = ChartBarDimensions;
//# sourceMappingURL=ChartBar.js.map