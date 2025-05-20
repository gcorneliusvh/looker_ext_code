import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var IdeDimensionGroup = React.forwardRef(function (props, ref) {
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
    d: "M6.88889 6H20.1111C21.15 6 22 6.9 22 8V14C22 15.1 21.15 16 20.1111 16H6.88889C5.85 16 5 15.1 5 14V8C5 6.9 5.85 6 6.88889 6ZM6.88889 14H20.1111V8H6.88889V14ZM3.88889 10C2.85 10 2 10.81 2 11.8V17.2C2 18.19 2.85 19 3.88889 19H17.1111C18.15 19 19 18.19 19 17.2H17.1111H3.88889V11.8V10Z"
  }));
});
IdeDimensionGroup.displayName = 'IdeDimensionGroup';
export var IdeDimensionGroupDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=IdeDimensionGroup.js.map