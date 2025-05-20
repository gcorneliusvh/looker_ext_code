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
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LookerEmbedSDK = void 0;
var embed_builder_1 = require("./embed_builder");
var dashboard_client_1 = require("./dashboard_client");
var explore_client_1 = require("./explore_client");
var extension_client_1 = require("./extension_client");
var look_client_1 = require("./look_client");
__exportStar(require("./types"), exports);
var LookerEmbedSDK = /** @class */ (function () {
    function LookerEmbedSDK() {
    }
    /**
     * Initialize the Embed SDK.
     *
     * @param apiHost The address or base URL of the Looker host (example.looker.com:9999, https://example.looker.com:9999)
     *                This is required for verification of messages sent from the embedded content.
     * @param authUrl A server endpoint that will sign SSO embed URLs
     */
    LookerEmbedSDK.init = function (apiHost, auth) {
        this.apiHost = apiHost;
        this.auth = typeof auth === 'string' ? { url: auth } : auth;
        this.acquireSession = undefined;
        this.generateTokens = undefined;
    };
    /**
     * Initialize the Embed SDK to use a cookieless session.
     *
     * @param apiHost The address or base URL of the host (example.looker.com:9999, https://example.looker.com:9999)
     * @param acquireSession is either a string containing a server endpoint that will acquire the embed session OR
     * a RequestInfo object for a fetch call to the server endpoint that will acquire the embed session OR
     * a callback that will invoke the server endpoint that will acquire the embed session.
     * The server endpoint must ultimately call the Looker endpoint `acquire_embed_cookieless_session`.
     * @param generateTokens is either a string containing a server endpoint that will generate new tokens OR
     * a RequestInfo object for a fetch call to the server endpoint that will generate new tokens OR
     * a callback that will invoke the server endpoint that will generate new tokens.
     * The server endpoint should ultimately call the Looker endpoint `generate_tokens_for_cookieless_session`.
     *
     * Looker 22.20+
     */
    LookerEmbedSDK.initCookieless = function (apiHost, acquireSession, generateTokens) {
        this.apiHost = apiHost;
        this.acquireSession = acquireSession;
        this.generateTokens = generateTokens;
        this.auth = undefined;
    };
    /**
     * Create an EmbedBuilder for an embedded Looker dashboard.
     *
     * @param url A signed SSO embed URL or embed URL for an already authenticated Looker user
     */
    LookerEmbedSDK.createDashboardWithUrl = function (url) {
        return new embed_builder_1.EmbedBuilder(this, 'dashboard', '/embed/dashboards', dashboard_client_1.LookerEmbedDashboard).withUrl(url);
    };
    /**
     * Create an EmbedBuilder for an embedded Looker dashboard.
     *
     * @param id The numeric ID of a Looker User Defined Dashboard, or LookML Dashboard ID
     */
    LookerEmbedSDK.createDashboardWithId = function (id) {
        return new embed_builder_1.EmbedBuilder(this, 'dashboard', '/embed/dashboards', dashboard_client_1.LookerEmbedDashboard).withId(id);
    };
    /**
     * Create an EmbedBuilder for an embedded Looker Explore.
     *
     * @param url A signed SSO embed URL or embed URL for an already authenticated Looker user
     */
    LookerEmbedSDK.createExploreWithUrl = function (url) {
        return new embed_builder_1.EmbedBuilder(this, 'explore', '/embed/explore', explore_client_1.LookerEmbedExplore).withUrl(url);
    };
    /**
     * Create an EmbedBuilder for an embedded Looker Explore.
     *
     * @param id The ID of a Looker explore
     */
    LookerEmbedSDK.createExploreWithId = function (id) {
        id = id.replace('::', '/'); // Handle old format explore ids.
        return new embed_builder_1.EmbedBuilder(this, 'explore', '/embed/explore', explore_client_1.LookerEmbedExplore).withId(id);
    };
    /**
     * Create an EmbedBuilder for an embedded Looker Look.
     *
     * @param url A signed SSO embed URL or embed URL for an already authenticated Looker user
     */
    LookerEmbedSDK.createLookWithUrl = function (url) {
        return new embed_builder_1.EmbedBuilder(this, 'look', '/embed/looks', look_client_1.LookerEmbedLook).withUrl(url);
    };
    /**
     * Create an EmbedBuilder for an embedded Looker dashboard.
     *
     * @param id The ID of a Looker Look
     */
    LookerEmbedSDK.createLookWithId = function (id) {
        return new embed_builder_1.EmbedBuilder(this, 'look', '/embed/looks', look_client_1.LookerEmbedLook).withId(id);
    };
    /**
     * Create an EmbedBuilder for an embedded Looker extension.
     *
     * @param url A signed SSO embed URL or embed URL for an already authenticated Looker user
     */
    LookerEmbedSDK.createExtensionWithUrl = function (url) {
        return new embed_builder_1.EmbedBuilder(this, 'extension', '/embed/extensions', extension_client_1.LookerEmbedExtension).withUrl(url);
    };
    /**
     * Create an EmbedBuilder for an embedded Looker extension. Requires Looker 7.12
     *
     * @param id The ID of a Looker Look
     */
    LookerEmbedSDK.createExtensionWithId = function (id) {
        return new embed_builder_1.EmbedBuilder(this, 'extension', '/embed/extensions', extension_client_1.LookerEmbedExtension).withId(id);
    };
    return LookerEmbedSDK;
}());
exports.LookerEmbedSDK = LookerEmbedSDK;
//# sourceMappingURL=index.js.map