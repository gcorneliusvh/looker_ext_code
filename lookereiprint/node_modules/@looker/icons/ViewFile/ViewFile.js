import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var ViewFile = React.forwardRef(function (props, ref) {
  var attrs = {
    "fill": "currentColor",
    "xmlns": "http://www.w3.org/2000/svg"
  };
  return React.createElement(StyledIconBase, _extends({
    iconAttrs: attrs,
    iconVerticalAlign: "middle",
    iconViewBox: "0 0 24 24"
  }, props, {
    ref: ref
  }), React.createElement("path", {
    d: "M20 4H4C2.9 4 2.01 4.9 2.01 6L2 18C2 19.1 2.9 20 4 20H20C21.1 20 22 19.1 22 18V6C22 4.9 21.1 4 20 4ZM20 18H15.5V10H20.0013L20 18ZM20.0013 9H15.5V6H20L20.0013 9ZM14.5 9H9.5V5.99867H14.5V9ZM8.49867 9H4.002V10H8.49867V17.9987H3.99867V5.99867H8.49867V9V9ZM9.5 10H14.5V17.9987H9.5V10V10Z"
  }));
});
ViewFile.displayName = 'ViewFile';
export var ViewFileDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=ViewFile.js.map