"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.IdeFileGenericDimensions = exports.IdeFileGeneric = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const IdeFileGeneric = _react.default.forwardRef((props, ref) => {
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
    d: "M17 5H19V7H17V5ZM15 5H5L5 19H19V9H16C15.4477 9 15 8.55228 15 8V5ZM3 5C3 3.89543 3.89543 3 5 3H19C20.1046 3 21 3.89543 21 5V19C21 20.1046 20.1046 21 19 21H5C3.89543 21 3 20.1046 3 19V5Z"
  }));
});

exports.IdeFileGeneric = IdeFileGeneric;
IdeFileGeneric.displayName = 'IdeFileGeneric';
const IdeFileGenericDimensions = {
  height: 24,
  width: 24
};
exports.IdeFileGenericDimensions = IdeFileGenericDimensions;
//# sourceMappingURL=IdeFileGeneric.js.map