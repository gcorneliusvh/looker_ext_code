"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ExpandCollapseDimensions = exports.ExpandCollapse = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const ExpandCollapse = _react.default.forwardRef((props, ref) => {
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
    d: "M13 8L13 11L3 11L3 13L13 13L13 16L17 12L13 8ZM19 20L21 20L21 4L19 4L19 20Z"
  }));
});

exports.ExpandCollapse = ExpandCollapse;
ExpandCollapse.displayName = 'ExpandCollapse';
const ExpandCollapseDimensions = {
  height: 24,
  width: 24
};
exports.ExpandCollapseDimensions = ExpandCollapseDimensions;
//# sourceMappingURL=ExpandCollapse.js.map