"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.VisPieDimensions = exports.VisPie = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const VisPie = _react.default.forwardRef((props, ref) => {
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
    d: "M18.1272 12.3825C18.1272 8.78649 15.2125 5.87183 11.6165 5.87183C11.4852 5.87183 11.3625 5.89849 11.2338 5.90783V5.87183H10.4678V7.48049C7.87249 7.85583 5.87183 10.0672 5.87183 12.7658C5.87183 15.7272 8.27249 18.1278 11.2338 18.1278C12.7018 18.1278 14.0165 17.5185 14.9838 16.5612L16.1318 17.3618L16.6032 16.7185L16.5132 16.6565C17.5138 15.5132 18.1272 14.0225 18.1272 12.3832V12.3825ZM11.2338 17.3618C8.69516 17.3618 6.63783 15.3038 6.63783 12.7658C6.63783 10.4892 8.29583 8.61316 10.4678 8.24716V13.3985L10.4605 13.4085L10.4678 13.4138L14.3492 16.1192C13.5285 16.8812 12.4425 17.3618 11.2338 17.3618ZM15.8832 16.2165L11.2338 12.9758V6.67649C11.3625 6.66716 11.4858 6.63849 11.6165 6.63849C14.7898 6.63849 17.3612 9.20983 17.3612 12.3832C17.3612 13.8592 16.7992 15.1998 15.8832 16.2165Z"
  }), _react.default.createElement("path", {
    d: "M19.9159 19.9147H4.08594V4.08466H19.9159V19.9147ZM4.59727 19.404H19.4059V4.59532H4.59727V19.404Z"
  }));
});

exports.VisPie = VisPie;
VisPie.displayName = 'VisPie';
const VisPieDimensions = {
  height: 24,
  width: 24
};
exports.VisPieDimensions = VisPieDimensions;
//# sourceMappingURL=VisPie.js.map