"use strict";
/*

 MIT License

 Copyright (c) 2019 Looker Data Sciences, Inc.

 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

 The above copyright notice and this permission notice shall be included in all
 copies or substantial portions of the Software.

 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 SOFTWARE.

 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __rest = (this && this.__rest) || function (s, e) {
    var t = {};
    for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p) && e.indexOf(p) < 0)
        t[p] = s[p];
    if (s != null && typeof Object.getOwnPropertySymbols === "function")
        for (var i = 0, p = Object.getOwnPropertySymbols(s); i < p.length; i++) {
            if (e.indexOf(p[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p[i]))
                t[p[i]] = s[p[i]];
        }
    return t;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.EmbedClient = void 0;
var chatty_1 = require("@looker/chatty");
var IS_URL = /^https?:\/\//;
/**
 * Wrapper for Looker embedded content. Provides a mechanism for creating the embedded content element,
 * and for establishing two-way communication between the parent window and the embedded content.
 */
var EmbedClient = /** @class */ (function () {
    /**
     * @hidden
     */
    function EmbedClient(_builder) {
        this._builder = _builder;
        this._hostBuilder = null;
        this._host = null;
        this._connection = null;
        this._client = null;
        this._cookielessInitialized = false;
    }
    Object.defineProperty(EmbedClient.prototype, "connection", {
        /**
         * Returns a promise that resolves to a client that can be used to send messages to the
         * embedded content.
         */
        get: function () {
            return this._connection;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(EmbedClient.prototype, "isConnected", {
        /**
         * Indicates whether two way communication has successfully been established with the embedded content.
         */
        get: function () {
            return !!this._connection;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(EmbedClient.prototype, "targetOrigin", {
        get: function () {
            if (this._builder.sandboxedHost) {
                return '*';
            }
            var apiHost = this._builder.apiHost;
            return IS_URL.test(apiHost) ? apiHost : "https://".concat(apiHost);
        },
        enumerable: false,
        configurable: true
    });
    EmbedClient.prototype.createIframe = function (url) {
        return __awaiter(this, void 0, void 0, function () {
            var eventType, _loop_1, this_1, _i, _a, handler, _b, _c, attr, _d, _e, attr;
            var _f;
            var _this = this;
            return __generator(this, function (_g) {
                this._hostBuilder = chatty_1.Chatty.createHost(url);
                if (this._builder.dialogScroll) {
                    this._builder.handlers['env:client:dialog'] = [
                        function (_a) {
                            var open = _a.open, placement = _a.placement;
                            // Placement of 'cover' means that the dialog top is close
                            // to the top of the IFRAME. The top MAY be scrolled out
                            // of view. The following attempts to scroll the top of the
                            // dialog into view.
                            if (open && placement === 'cover') {
                                // Timeout is a little ugly. Suspect there might be an issue
                                // with a Looker component where the last row is scrolled
                                // into view. Normally not an issue because outside of embed
                                // as the dialog is limited to the viewport.
                                // Make timeout configurable?
                                window.setTimeout(function () {
                                    if (_this._host) {
                                        _this._host.iframe.scrollIntoView(true);
                                    }
                                }, 200);
                            }
                        },
                    ];
                }
                if (this._builder.dynamicIFrameHeight) {
                    this._builder.handlers['page:properties:changed'] = [
                        function (_a) {
                            var height = _a.height;
                            if (height && height > 100 && _this._host) {
                                _this._host.iframe.style.height = "".concat(height, "px");
                            }
                        },
                    ];
                }
                if (this._builder.isCookielessEmbed) {
                    this._builder.handlers['session:tokens:request'] = [
                        function () { return __awaiter(_this, void 0, void 0, function () {
                            var _a, api_token, api_token_ttl, navigation_token, navigation_token_ttl, session_reference_token_ttl, client;
                            return __generator(this, function (_b) {
                                switch (_b.label) {
                                    case 0:
                                        if (!(this._client &&
                                            this._cookielessApiToken &&
                                            this._builder.generateTokens)) return [3 /*break*/, 4];
                                        if (!this._cookielessInitialized) return [3 /*break*/, 2];
                                        return [4 /*yield*/, this.generateTokens()];
                                    case 1:
                                        _a = _b.sent(), api_token = _a.api_token, api_token_ttl = _a.api_token_ttl, navigation_token = _a.navigation_token, navigation_token_ttl = _a.navigation_token_ttl, session_reference_token_ttl = _a.session_reference_token_ttl;
                                        this._cookielessApiToken = api_token;
                                        this._cookielessApiTokenTtl = api_token_ttl;
                                        this._cookielessNavigationToken = navigation_token;
                                        this._cookielessNavigationTokenTtl = navigation_token_ttl;
                                        this._cookielessSessionReferenceTokenTtl =
                                            session_reference_token_ttl;
                                        return [3 /*break*/, 3];
                                    case 2:
                                        this._cookielessInitialized = true;
                                        _b.label = 3;
                                    case 3:
                                        client = this._client;
                                        client.send('session:tokens', {
                                            api_token: this._cookielessApiToken,
                                            api_token_ttl: this._cookielessApiTokenTtl,
                                            navigation_token: this._cookielessNavigationToken,
                                            navigation_token_ttl: this._cookielessNavigationTokenTtl,
                                            session_reference_token_ttl: this._cookielessSessionReferenceTokenTtl,
                                        });
                                        _b.label = 4;
                                    case 4: return [2 /*return*/];
                                }
                            });
                        }); },
                    ];
                }
                for (eventType in this._builder.handlers) {
                    _loop_1 = function (handler) {
                        this_1._hostBuilder.on(eventType, function () {
                            var args = [];
                            for (var _i = 0; _i < arguments.length; _i++) {
                                args[_i] = arguments[_i];
                            }
                            return handler.apply(_this._client, args);
                        });
                    };
                    this_1 = this;
                    for (_i = 0, _a = this._builder.handlers[eventType]; _i < _a.length; _i++) {
                        handler = _a[_i];
                        _loop_1(handler);
                    }
                }
                for (_b = 0, _c = this._builder.sandboxAttrs; _b < _c.length; _b++) {
                    attr = _c[_b];
                    this._hostBuilder.withSandboxAttribute(attr);
                }
                for (_d = 0, _e = this._builder.allowAttrs; _d < _e.length; _d++) {
                    attr = _e[_d];
                    this._hostBuilder.withAllowAttribute(attr);
                }
                this._host = this._hostBuilder
                    // tslint:disable-next-line:deprecation
                    .frameBorder(this._builder.frameBorder)
                    .withTargetOrigin(this.targetOrigin)
                    .appendTo(this._builder.el)
                    .build();
                // IE doesn't like calling classList.add() with no arguments, so check
                if (this._builder.classNames.length) {
                    (_f = this._host.iframe.classList).add.apply(_f, this._builder.classNames);
                }
                if (this._builder.scrollMonitor) {
                    this.addIframeMonitor(this._host.iframe);
                }
                return [2 /*return*/, this._host.connect().then(function (host) {
                        // eslint-disable-next-line new-cap
                        _this._client = new _this._builder.clientConstructor(host);
                        return _this._client;
                    })];
            });
        });
    };
    EmbedClient.prototype.sendScrollData = function (iframe) {
        var client = this._client;
        client.send('env:host:scroll', {
            offsetLeft: iframe.offsetLeft,
            offsetTop: iframe.offsetTop,
            scrollX: window.scrollX,
            scrollY: window.scrollY,
        });
    };
    EmbedClient.prototype.addIframeMonitor = function (iframe) {
        var _this = this;
        document.addEventListener('scroll', function (_event) {
            _this.sendScrollData(iframe);
        });
        window.addEventListener('resize', function (_event) {
            _this.sendScrollData(iframe);
        });
    };
    EmbedClient.prototype.createUrl = function () {
        return __awaiter(this, void 0, void 0, function () {
            var src, auth, url, _i, _a, param;
            var _this = this;
            return __generator(this, function (_b) {
                src = this._builder.embedUrl;
                auth = this._builder.auth;
                if (!(auth === null || auth === void 0 ? void 0 : auth.url))
                    return [2 /*return*/, "".concat(this._builder.apiHost).concat(src)];
                url = "".concat(auth.url, "?src=").concat(encodeURIComponent(src));
                if (auth.params) {
                    for (_i = 0, _a = auth.params; _i < _a.length; _i++) {
                        param = _a[_i];
                        url += "&".concat(encodeURIComponent(param.name), "=").concat(encodeURIComponent(param.value));
                    }
                }
                // eslint-disable-next-line no-async-promise-executor
                return [2 /*return*/, new Promise(function (resolve, reject) { return __awaiter(_this, void 0, void 0, function () {
                        var xhr, _i, _a, header;
                        return __generator(this, function (_b) {
                            xhr = new XMLHttpRequest();
                            xhr.open('GET', url);
                            if (auth.withCredentials) {
                                xhr.withCredentials = auth.withCredentials;
                            }
                            xhr.setRequestHeader('Cache-Control', 'no-cache');
                            if (auth.headers) {
                                for (_i = 0, _a = auth.headers; _i < _a.length; _i++) {
                                    header = _a[_i];
                                    xhr.setRequestHeader(header.name, header.value);
                                }
                            }
                            xhr.onload = function () {
                                if (xhr.status === 200) {
                                    resolve(JSON.parse(xhr.responseText).url);
                                }
                                else {
                                    reject(xhr.statusText);
                                }
                            };
                            xhr.onerror = function () { return reject(xhr.statusText); };
                            xhr.send();
                            return [2 /*return*/];
                        });
                    }); })];
            });
        });
    };
    EmbedClient.prototype.acquireCookielessEmbedSession = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (EmbedClient.sessionAcquired) {
                            return [2 /*return*/, this.acquireCookielessEmbedSessionInternal()];
                        }
                        if (!EmbedClient.acquireSessionPromise) return [3 /*break*/, 2];
                        return [4 /*yield*/, EmbedClient.acquireSessionPromise];
                    case 1:
                        _a.sent();
                        return [2 /*return*/, this.acquireCookielessEmbedSessionInternal()];
                    case 2:
                        EmbedClient.acquireSessionPromise =
                            this.acquireCookielessEmbedSessionInternal();
                        return [2 /*return*/, EmbedClient.acquireSessionPromise
                                .then(function (url) {
                                EmbedClient.sessionAcquired = true;
                                return url;
                            })
                                .catch(function (error) {
                                EmbedClient.acquireSessionPromise = undefined;
                                throw error;
                            })];
                }
            });
        });
    };
    EmbedClient.prototype.acquireCookielessEmbedSessionInternal = function () {
        return __awaiter(this, void 0, void 0, function () {
            var _a, acquireSession, generateTokens, _b, authentication_token, api_token, api_token_ttl, navigation_token, navigation_token_ttl, session_reference_token_ttl, apiHost, sep, src, embedPath;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        _a = this._builder, acquireSession = _a.acquireSession, generateTokens = _a.generateTokens;
                        if (!acquireSession) {
                            throw new Error('invalid state: acquireSession not defined');
                        }
                        if (!generateTokens) {
                            throw new Error('invalid state: generateTokens not defined');
                        }
                        return [4 /*yield*/, this.acquireSession()];
                    case 1:
                        _b = _c.sent(), authentication_token = _b.authentication_token, api_token = _b.api_token, api_token_ttl = _b.api_token_ttl, navigation_token = _b.navigation_token, navigation_token_ttl = _b.navigation_token_ttl, session_reference_token_ttl = _b.session_reference_token_ttl;
                        if (!authentication_token || !navigation_token || !api_token) {
                            throw new Error('failed to prepare cookieless embed session');
                        }
                        this._cookielessApiToken = api_token;
                        this._cookielessApiTokenTtl = api_token_ttl;
                        this._cookielessNavigationToken = navigation_token;
                        this._cookielessNavigationTokenTtl = navigation_token_ttl;
                        this._cookielessSessionReferenceTokenTtl = session_reference_token_ttl;
                        apiHost = "https://".concat(this._builder.apiHost);
                        sep = new URL(this._builder.embedUrl, apiHost).search === '' ? '?' : '&';
                        src = "".concat(this._builder.embedUrl).concat(sep, "embed_navigation_token=").concat(navigation_token);
                        embedPath = '/login/embed/' +
                            encodeURIComponent(src) +
                            "?embed_authentication_token=".concat(authentication_token);
                        return [2 /*return*/, "".concat(apiHost).concat(embedPath)];
                }
            });
        });
    };
    EmbedClient.prototype.acquireSession = function () {
        return __awaiter(this, void 0, void 0, function () {
            var acquireSession, _a, url, init, resp, error_1;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        acquireSession = this._builder.acquireSession;
                        if (!(typeof acquireSession === 'function')) return [3 /*break*/, 2];
                        return [4 /*yield*/, acquireSession()];
                    case 1: return [2 /*return*/, _b.sent()];
                    case 2:
                        _b.trys.push([2, 5, , 6]);
                        _a = this.getResource(acquireSession), url = _a.url, init = _a.init;
                        return [4 /*yield*/, fetch(url, init)];
                    case 3:
                        resp = _b.sent();
                        if (!resp.ok) {
                            console.error('acquire embed session failed', { resp: resp });
                            throw new Error("acquire embed session failed");
                        }
                        return [4 /*yield*/, resp.json()];
                    case 4: return [2 /*return*/, (_b.sent())];
                    case 5:
                        error_1 = _b.sent();
                        console.error(error_1);
                        throw new Error("acquire embed session failed");
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    EmbedClient.prototype.generateTokens = function () {
        return __awaiter(this, void 0, void 0, function () {
            var generateTokens, _a, url, defaultInit, init, resp, error_2;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        generateTokens = this._builder.generateTokens;
                        if (!(typeof generateTokens === 'function')) return [3 /*break*/, 2];
                        return [4 /*yield*/, generateTokens()];
                    case 1: return [2 /*return*/, _b.sent()];
                    case 2:
                        _b.trys.push([2, 5, , 6]);
                        _a = this.getResource(generateTokens), url = _a.url, defaultInit = _a.init;
                        init = defaultInit || {
                            body: JSON.stringify({
                                api_token: this._cookielessApiToken,
                                navigation_token: this._cookielessNavigationToken,
                            }),
                            headers: {
                                'content-type': 'application/json',
                            },
                            method: 'PUT',
                        };
                        return [4 /*yield*/, fetch(url, init)];
                    case 3:
                        resp = _b.sent();
                        if (!resp.ok) {
                            if (resp.status === 400) {
                                return [2 /*return*/, { session_reference_token_ttl: 0 }];
                            }
                            console.error('generate tokens failed', { resp: resp });
                            throw new Error("generate tokens failed");
                        }
                        return [4 /*yield*/, resp.json()];
                    case 4: return [2 /*return*/, (_b.sent())];
                    case 5:
                        error_2 = _b.sent();
                        console.error(error_2);
                        throw new Error("generate tokens failed");
                    case 6: return [2 /*return*/];
                }
            });
        });
    };
    EmbedClient.prototype.getResource = function (resource) {
        var url;
        var init;
        if (typeof resource === 'object') {
            var tempUrl = resource.url, rest = __rest(resource, ["url"]);
            init = rest;
            url = tempUrl;
        }
        else {
            url = resource;
        }
        return { init: init, url: url };
    };
    /**
     * Establish two way communication with embedded content. Returns a promise that resolves to a
     * client that can be used to send messages to the embedded content.
     */
    EmbedClient.prototype.connect = function () {
        return __awaiter(this, void 0, void 0, function () {
            var _this = this;
            return __generator(this, function (_a) {
                if (this._connection)
                    return [2 /*return*/, this._connection];
                if (this._builder.url) {
                    if (this._builder.isCookielessEmbed) {
                        throw new Error('withUrl not supported by cookieless embed');
                    }
                    this._connection = this.createIframe(this._builder.url);
                }
                else {
                    if (this._builder.isCookielessEmbed) {
                        this._connection = this.acquireCookielessEmbedSession()
                            .then(function (url) { return __awaiter(_this, void 0, void 0, function () { return __generator(this, function (_a) {
                            return [2 /*return*/, this.createIframe(url)];
                        }); }); })
                            .catch(function (_error) {
                            _this._connection = null;
                            throw _error;
                        });
                    }
                    else {
                        this._connection = this.createUrl().then(function (url) { return __awaiter(_this, void 0, void 0, function () { return __generator(this, function (_a) {
                            return [2 /*return*/, this.createIframe(url)];
                        }); }); });
                    }
                }
                return [2 /*return*/, this._connection];
            });
        });
    };
    EmbedClient.sessionAcquired = false;
    return EmbedClient;
}());
exports.EmbedClient = EmbedClient;
//# sourceMappingURL=embed.js.map