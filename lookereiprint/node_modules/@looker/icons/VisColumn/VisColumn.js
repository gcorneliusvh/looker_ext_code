import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var VisColumn = React.forwardRef(function (props, ref) {
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
    d: "M5.8728 10.4683H8.9368V18.1276H5.8728V10.4683Z"
  }), React.createElement("path", {
    d: "M10.4688 12H13.5328V18.1273H10.4688V12Z"
  }), React.createElement("path", {
    d: "M15.0651 5.87238H18.1291V18.1277H15.0651V5.87238Z"
  }), React.createElement("path", {
    d: "M19.9159 19.9147H4.08594V4.08466H19.9159V19.9147ZM4.59727 19.404H19.4059V4.59532H4.59727V19.404Z"
  }));
});
VisColumn.displayName = 'VisColumn';
export var VisColumnDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=VisColumn.js.map