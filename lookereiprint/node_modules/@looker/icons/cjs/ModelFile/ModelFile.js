"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ModelFileDimensions = exports.ModelFile = void 0;

var _react = _interopRequireDefault(require("react"));

var _styledIcon = require("@styled-icons/styled-icon");

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

const ModelFile = _react.default.forwardRef((props, ref) => {
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
    d: "M19.5 4H4.5C3.12467 4 2 4.9 2 6V18C2 19.1 3.12533 20 4.5 20H19.5C20.8747 20 22 19.1 22 18V6C22 4.9 20.8747 4 19.5 4ZM19.26 13H4.76V11H19.26V13ZM14.26 17H4.76V15H14.26V17ZM19.26 9H4.76V7H19.26V9Z"
  }));
});

exports.ModelFile = ModelFile;
ModelFile.displayName = 'ModelFile';
const ModelFileDimensions = {
  height: 24,
  width: 24
};
exports.ModelFileDimensions = ModelFileDimensions;
//# sourceMappingURL=ModelFile.js.map