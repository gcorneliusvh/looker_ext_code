"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.FieldTierDimensions = exports.FieldTier = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const FieldTier = _react.default.forwardRef((props, ref) => {
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
    d: "M21 5H3V7H21V5ZM21 9H7V11H21V9ZM11 13H21V15H11V13ZM21 17H15V19H21V17Z"
  }));
});

exports.FieldTier = FieldTier;
FieldTier.displayName = 'FieldTier';
const FieldTierDimensions = {
  height: 24,
  width: 24
};
exports.FieldTierDimensions = FieldTierDimensions;
//# sourceMappingURL=FieldTier.js.map