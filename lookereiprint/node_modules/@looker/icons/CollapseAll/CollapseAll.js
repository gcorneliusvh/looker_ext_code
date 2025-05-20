import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var CollapseAll = React.forwardRef(function (props, ref) {
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
    d: "M16 5H8L12 9L16 5Z"
  }), React.createElement("path", {
    d: "M4 11H20V13H4V11Z"
  }), React.createElement("path", {
    d: "M8 19L16 19L12 15L8 19Z"
  }));
});
CollapseAll.displayName = 'CollapseAll';
export var CollapseAllDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=CollapseAll.js.map