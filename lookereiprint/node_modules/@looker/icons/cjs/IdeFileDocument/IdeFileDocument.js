"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.IdeFileDocumentDimensions = exports.IdeFileDocument = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const IdeFileDocument = _react.default.forwardRef((props, ref) => {
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
    d: "M19 5H5L5 19H19V5ZM5 3C3.89543 3 3 3.89543 3 5V19C3 20.1046 3.89543 21 5 21H19C20.1046 21 21 20.1046 21 19V5C21 3.89543 20.1046 3 19 3H5Z"
  }), _react.default.createElement("rect", {
    x: 7,
    y: 8,
    width: 10,
    height: 2,
    rx: 1
  }), _react.default.createElement("rect", {
    x: 7,
    y: 12,
    width: 10,
    height: 2,
    rx: 1
  }));
});

exports.IdeFileDocument = IdeFileDocument;
IdeFileDocument.displayName = 'IdeFileDocument';
const IdeFileDocumentDimensions = {
  height: 24,
  width: 24
};
exports.IdeFileDocumentDimensions = IdeFileDocumentDimensions;
//# sourceMappingURL=IdeFileDocument.js.map