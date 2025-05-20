"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.DownloadDimensions = exports.Download = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const Download = _react.default.forwardRef((props, ref) => {
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
    d: "M13 5V11H14.17L12 13.17L9.83 11H11V5H13ZM15 3H9V9H5L12 16L19 9H15V3ZM19 18H5V20H19V18Z"
  }));
});

exports.Download = Download;
Download.displayName = 'Download';
const DownloadDimensions = {
  height: 24,
  width: 24
};
exports.DownloadDimensions = DownloadDimensions;
//# sourceMappingURL=Download.js.map