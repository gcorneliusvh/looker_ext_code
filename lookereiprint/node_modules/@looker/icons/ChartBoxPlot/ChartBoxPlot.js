import "core-js/modules/es.object.assign.js";

function _extends() { _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; }; return _extends.apply(this, arguments); }

import React from 'react';
import { StyledIconBase } from '@styled-icons/styled-icon';
export var ChartBoxPlot = React.forwardRef(function (props, ref) {
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
    d: "M5 11H8V18H5V11ZM10.5 5H13.5V14H10.5V5ZM16 7H19V17H16V7Z"
  }));
});
ChartBoxPlot.displayName = 'ChartBoxPlot';
export var ChartBoxPlotDimensions = {
  height: 24,
  width: 24
};
//# sourceMappingURL=ChartBoxPlot.js.map