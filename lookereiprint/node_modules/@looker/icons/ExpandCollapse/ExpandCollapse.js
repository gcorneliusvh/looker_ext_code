import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var ExpandCollapse = React.forwardRef(function (props, ref) {
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
    d: "M13 8L13 11L3 11L3 13L13 13L13 16L17 12L13 8ZM19 20L21 20L21 4L19 4L19 20Z"
  }));
});
ExpandCollapse.displayName = 'ExpandCollapse';
export var ExpandCollapseDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=ExpandCollapse.js.map