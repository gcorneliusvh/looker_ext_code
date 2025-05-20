import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var IdeParameter = React.forwardRef(function (props, ref) {
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
    d: "M5 5L12.0127 14L19 5H5ZM5 9C3.9 9 3 9.9 3 11V17C3 18.1 3.9 19 5 19H19C20.1 19 21 18.1 21 17V11C21 9.9 20.1 9 19 9H18.1719L16.625 11H19V17H5V11H7.375L5.8125 9H5Z"
  }));
});
IdeParameter.displayName = 'IdeParameter';
export var IdeParameterDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=IdeParameter.js.map