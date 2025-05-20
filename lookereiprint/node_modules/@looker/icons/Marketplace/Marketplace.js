import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var Marketplace = React.forwardRef(function (props, ref) {
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
    d: "M18.36 5L18.96 10H5.04L5.64 5H18.36ZM20 3H4L3 10V12H4V21H14V12H18V21H20V12H21V10L20 3ZM6 19V12H12V19H6Z"
  }));
});
Marketplace.displayName = 'Marketplace';
export var MarketplaceDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=Marketplace.js.map