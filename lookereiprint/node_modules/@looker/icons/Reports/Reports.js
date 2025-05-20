import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var Reports = React.forwardRef(function (props, ref) {
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
    d: "M19 3H9V17H19V3ZM9 1C7.89543 1 7 1.89543 7 3V17C7 18.1046 7.89543 19 9 19H19C20.1046 19 21 18.1046 21 17V3C21 1.89543 20.1046 1 19 1H9ZM3 9C3 7.89543 3.89543 7 5 7V21H16C16 22.1046 15.1046 23 14 23H5C3.89543 23 3 22.1046 3 21V9ZM15 5H11V7H15V5ZM11 9H17V11H11V9ZM17 13H11V15H17V13Z"
  }));
});
Reports.displayName = 'Reports';
export var ReportsDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=Reports.js.map