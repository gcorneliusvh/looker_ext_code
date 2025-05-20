"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.VisLineDimensions = exports.VisLine = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const VisLine = _react.default.forwardRef((props, ref) => {
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
    d: "M12.1452 14.0806L8.93588 12.4759L6.06255 13.9126L5.68188 13.1513L8.93588 11.5239L11.8539 12.9833L14.7719 8.60592L17.9366 7.02325L18.3172 7.78459L15.3546 9.26592L12.1446 14.0799L12.1452 14.0806Z"
  }));
});

exports.VisLine = VisLine;
VisLine.displayName = 'VisLine';
const VisLineDimensions = {
  height: 24,
  width: 24
};
exports.VisLineDimensions = VisLineDimensions;
//# sourceMappingURL=VisLine.js.map