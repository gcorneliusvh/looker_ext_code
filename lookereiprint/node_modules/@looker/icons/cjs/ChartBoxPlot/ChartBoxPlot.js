"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ChartBoxPlotDimensions = exports.ChartBoxPlot = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const ChartBoxPlot = _react.default.forwardRef((props, ref) => {
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
    d: "M5 11H8V18H5V11ZM10.5 5H13.5V14H10.5V5ZM16 7H19V17H16V7Z"
  }));
});

exports.ChartBoxPlot = ChartBoxPlot;
ChartBoxPlot.displayName = 'ChartBoxPlot';
const ChartBoxPlotDimensions = {
  height: 24,
  width: 24
};
exports.ChartBoxPlotDimensions = ChartBoxPlotDimensions;
//# sourceMappingURL=ChartBoxPlot.js.map