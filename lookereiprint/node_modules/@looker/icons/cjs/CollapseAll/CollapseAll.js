"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.CollapseAllDimensions = exports.CollapseAll = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const CollapseAll = _react.default.forwardRef((props, ref) => {
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
    d: "M16 5H8L12 9L16 5Z"
  }), _react.default.createElement("path", {
    d: "M4 11H20V13H4V11Z"
  }), _react.default.createElement("path", {
    d: "M8 19L16 19L12 15L8 19Z"
  }));
});

exports.CollapseAll = CollapseAll;
CollapseAll.displayName = 'CollapseAll';
const CollapseAllDimensions = {
  height: 24,
  width: 24
};
exports.CollapseAllDimensions = CollapseAllDimensions;
//# sourceMappingURL=CollapseAll.js.map