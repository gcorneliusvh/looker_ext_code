"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.PivotDimensions = exports.Pivot = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const Pivot = _react.default.forwardRef((props, ref) => {
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
    d: "M17 3L21.3301 7.5H18V16C18 17.1046 17.1046 18 16 18H7.5V21.3301L3 17L7.5 12.6699V16H16V7.5H12.6699L17 3Z"
  }));
});

exports.Pivot = Pivot;
Pivot.displayName = 'Pivot';
const PivotDimensions = {
  height: 24,
  width: 24
};
exports.PivotDimensions = PivotDimensions;
//# sourceMappingURL=Pivot.js.map