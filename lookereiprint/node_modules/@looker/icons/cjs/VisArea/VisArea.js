"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.VisAreaDimensions = exports.VisArea = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const VisArea = _react.default.forwardRef((props, ref) => {
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
    d: "M19.9159 19.9147H4.08594V4.08466H19.9159V19.9147ZM4.59727 19.404H19.4059V4.59532H4.59727V19.404Z"
  }), _react.default.createElement("path", {
    d: "M18.1284 7.40363L15.0644 8.93563L12.0004 13.5316L8.93638 11.9996L5.87238 13.5316V18.1276H18.1277V7.40429L18.1284 7.40363Z"
  }));
});

exports.VisArea = VisArea;
VisArea.displayName = 'VisArea';
const VisAreaDimensions = {
  height: 24,
  width: 24
};
exports.VisAreaDimensions = VisAreaDimensions;
//# sourceMappingURL=VisArea.js.map