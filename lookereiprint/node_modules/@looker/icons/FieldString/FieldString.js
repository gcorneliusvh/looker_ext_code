import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var FieldString = React.forwardRef(function (props, ref) {
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
    d: "M10.6119 4.24805H13.3619L19.3239 20H16.6619L15.2099 15.952H8.78588L7.33388 20H4.67188L10.6119 4.24805ZM14.4179 13.708L12.6579 8.89005L12.0639 7.08605H11.9319L11.3379 8.89005L9.57788 13.708H14.4179Z"
  }));
});
FieldString.displayName = 'FieldString';
export var FieldStringDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=FieldString.js.map