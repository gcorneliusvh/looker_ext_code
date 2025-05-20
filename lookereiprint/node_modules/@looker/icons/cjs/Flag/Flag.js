"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.FlagDimensions = exports.Flag = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const Flag = _react.default.forwardRef((props, ref) => {
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
    d: "M17 8.5L21 3H3V14H21L17 8.5ZM17.0725 12L14.527 8.5L17.0725 5H5V12H17.0725Z"
  }), _react.default.createElement("path", {
    fillRule: "evenodd",
    clipRule: "evenodd",
    d: "M3 20L3 14H5L5 20H3Z"
  }));
});

exports.Flag = Flag;
Flag.displayName = 'Flag';
const FlagDimensions = {
  height: 24,
  width: 24
};
exports.FlagDimensions = FlagDimensions;
//# sourceMappingURL=Flag.js.map