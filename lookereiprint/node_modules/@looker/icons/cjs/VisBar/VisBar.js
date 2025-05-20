"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.VisBarDimensions = exports.VisBar = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const VisBar = _react.default.forwardRef((props, ref) => {
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
    d: "M5.8728 5.8728H18.1281V7.91547H5.8728V5.8728Z"
  }), _react.default.createElement("path", {
    d: "M5.8728 16.085H8.9368V18.1277H5.8728V16.085Z"
  }), _react.default.createElement("path", {
    d: "M5.8728 9.27698H15.0641V11.3196H5.8728V9.27698Z"
  }), _react.default.createElement("path", {
    d: "M5.8728 12.6805H12.0001V14.7231H5.8728V12.6805Z"
  }), _react.default.createElement("path", {
    d: "M19.9159 19.9147H4.08594V4.08466H19.9159V19.9147ZM4.59727 19.404H19.4059V4.59532H4.59727V19.404Z"
  }));
});

exports.VisBar = VisBar;
VisBar.displayName = 'VisBar';
const VisBarDimensions = {
  height: 24,
  width: 24
};
exports.VisBarDimensions = VisBarDimensions;
//# sourceMappingURL=VisBar.js.map