import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var QuickStart = React.forwardRef(function (props, ref) {
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
    d: "M8.74926 4.03571C8.95031 3.41806 9.52605 3 10.1756 3H15.4881C16.6142 3 17.3388 4.19459 16.8184 5.19321L14.9613 8.75676H16.2959C17.341 8.75676 17.9246 9.96284 17.2762 10.7824L9.40177 20.735C8.65822 21.6747 7.16109 20.9168 7.47839 19.7612L9.00094 14.2162H7.50111C6.48172 14.2162 5.75925 13.2213 6.07478 12.2519L8.74926 4.03571ZM10.5387 5L8.18969 12.2162H11.6241L10.5785 16.0244L14.7462 10.7568H11.6638L14.6638 5H10.5387Z"
  }));
});
QuickStart.displayName = 'QuickStart';
export var QuickStartDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=QuickStart.js.map