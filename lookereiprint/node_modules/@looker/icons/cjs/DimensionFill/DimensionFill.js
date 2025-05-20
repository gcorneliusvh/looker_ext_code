"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.DimensionFillDimensions = exports.DimensionFill = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const DimensionFill = _react.default.forwardRef((props, ref) => {
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
  }), _react.default.createElement("rect", {
    x: 8,
    y: 4,
    width: 14,
    height: 3
  }), _react.default.createElement("path", {
    fillRule: "evenodd",
    clipRule: "evenodd",
    d: "M2 9H17V15H2V9ZM4 11V13H15V11H4Z"
  }), _react.default.createElement("rect", {
    x: 8,
    y: 17,
    width: 14,
    height: 3
  }));
});

exports.DimensionFill = DimensionFill;
DimensionFill.displayName = 'DimensionFill';
const DimensionFillDimensions = {
  height: 24,
  width: 24
};
exports.DimensionFillDimensions = DimensionFillDimensions;
//# sourceMappingURL=DimensionFill.js.map