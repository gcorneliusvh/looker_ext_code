"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.ProxySession = void 0;
var _authSession = require("./authSession");
var _browserTransport = require("./browserTransport");
var _transport = require("./transport");
function asyncGeneratorStep(gen, resolve, reject, _next, _throw, key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { Promise.resolve(value).then(_next, _throw); } }
function _asyncToGenerator(fn) { return function () { var self = this, args = arguments; return new Promise(function (resolve, reject) { var gen = fn.apply(self, args); function _next(value) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "next", value); } function _throw(err) { asyncGeneratorStep(gen, resolve, reject, _next, _throw, "throw", err); } _next(undefined); }); }; }
class ProxySession extends _authSession.AuthSession {
  constructor(settings, proxyUrl, transport) {
    super(settings, transport || new _browserTransport.BrowserTransport(settings));
    this.settings = settings;
    this.proxyUrl = proxyUrl;
  }
  isAuthenticated() {
    return true;
  }
  authenticate(props) {
    var _this = this;
    return _asyncToGenerator(function* () {
      if (!props.headers) {
        props.headers = {};
      }
      props.headers['X-Forwarded-For'] = props.url;
      props.headers[_transport.LookerAppId] = _transport.agentPrefix;
      props.url = _this.proxyUrl;
      return props;
    })();
  }
}
exports.ProxySession = ProxySession;
//# sourceMappingURL=proxySession.js.map