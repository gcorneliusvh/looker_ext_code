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
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        if (typeof b !== "function" && b !== null)
            throw new TypeError("Class extends value " + String(b) + " is not a constructor or null");
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.LookerEmbedLook = void 0;
var embed_base_1 = require("./embed_base");
/**
 * Client that communicates with an embedded Looker Look. Messages are documented
 * [here](https://docs.looker.com/r/sdk/events)
 */
var LookerEmbedLook = /** @class */ (function (_super) {
    __extends(LookerEmbedLook, _super);
    function LookerEmbedLook() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    /**
     * Convenience method for sending a run message to the embedded Look.
     */
    LookerEmbedLook.prototype.run = function () {
        this.send('look:run');
    };
    /**
     * Convenience method for updating the filters of the embedded Look.
     *
     * @param filters A set of filter parameters to update
     */
    LookerEmbedLook.prototype.updateFilters = function (params) {
        this.send('look:filters:update', { filters: params });
    };
    return LookerEmbedLook;
}(embed_base_1.LookerEmbedBase));
exports.LookerEmbedLook = LookerEmbedLook;
//# sourceMappingURL=look_client.js.map