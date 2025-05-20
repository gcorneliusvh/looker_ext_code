"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ChartTimelineDimensions = exports.ChartTimeline = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const ChartTimeline = _react.default.forwardRef((props, ref) => {
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
    d: "M19 13H5V11H19V13ZM21 17H7V15H21V17ZM17 7V9H3V7H17Z"
  }));
});

exports.ChartTimeline = ChartTimeline;
ChartTimeline.displayName = 'ChartTimeline';
const ChartTimelineDimensions = {
  height: 24,
  width: 24
};
exports.ChartTimelineDimensions = ChartTimelineDimensions;
//# sourceMappingURL=ChartTimeline.js.map