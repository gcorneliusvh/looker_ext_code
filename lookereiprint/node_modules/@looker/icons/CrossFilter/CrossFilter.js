import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var CrossFilter = React.forwardRef(function (props, ref) {
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
    fillRule: "evenodd",
    clipRule: "evenodd",
    d: "M22.1599 11.2208C22.1599 6.12851 18.0959 2 13.0832 2C12.9649 2 12.8473 2.00384 12.7305 2.00818V12.0255H22.1247C22.1478 11.76 22.1599 11.4919 22.1599 11.2208ZM11.0767 21.998C15.8227 21.998 19.717 18.2977 20.1187 13.5817H10.7245V3.56445C5.87466 3.75296 2 7.80499 2 12.7773C2 17.8696 6.064 21.998 11.0767 21.998Z"
  }));
});
CrossFilter.displayName = 'CrossFilter';
export var CrossFilterDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=CrossFilter.js.map