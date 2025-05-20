import React from 'react';
export var ErrorMessage = _ref => {
  var {
    errorMessage
  } = _ref;
  return React.createElement("div", {
    id: "extension-initialization-error",
    "aria-live": "polite",
    role: "status",
    style: {
      width: '100%',
      margin: '-8px',
      padding: '12px 16px',
      background: '#FFE5E9',
      borderRadius: '4px',
      font: '0.875rem "Helvetica", san-serif'
    }
  }, errorMessage);
};
//# sourceMappingURL=ErrorMessage.js.map