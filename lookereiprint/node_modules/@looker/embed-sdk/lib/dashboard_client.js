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
Object.defineProperty(exports, "__esModule", { value: true });
exports.LookerEmbedDashboard = void 0;
var embed_base_1 = require("./embed_base");
/**
 * Client that communicates with an embedded Looker dashboard. Messages are documented
 * [here](https://docs.looker.com/r/sdk/events)
 */
var LookerEmbedDashboard = /** @class */ (function (_super) {
    __extends(LookerEmbedDashboard, _super);
    function LookerEmbedDashboard() {
        return _super !== null && _super.apply(this, arguments) || this;
    }
    /**
     * Convenience method for sending a run message to the embedded dashboard.
     */
    LookerEmbedDashboard.prototype.run = function () {
        this.send('dashboard:run');
    };
    /**
     * Convenience method for sending a stop message to the embedded dashboard.
     *
     * Requires Looker 7.14 and Dashboards Next (see [[EmbedBuilder.withNext]]).
     */
    LookerEmbedDashboard.prototype.stop = function () {
        this.send('dashboard:stop');
    };
    /**
     * Convenience method for sending an edit message to the embedded dashboard.
     *
     * Requires Looker 22.20 and Dashboards Next (see [[EmbedBuilder.withNext]]).
     */
    LookerEmbedDashboard.prototype.edit = function () {
        this.send('dashboard:edit');
    };
    /**
     * Convenience method for updating the filters of the embedded dashboard.
     *
     * @param filters A set of filter parameters to update
     */
    LookerEmbedDashboard.prototype.updateFilters = function (params) {
        this.send('dashboard:filters:update', { filters: params });
    };
    /**
     * Convenience method for setting options on the embedded dashboard.
     *
     * @param options An options object to be applied
     */
    LookerEmbedDashboard.prototype.setOptions = function (options) {
        this.send('dashboard:options:set', options);
    };
    /**
     * Convenience method for opening the dashboard schedule dialog.
     *
     * Requires Looker 7.18 and Dashboards Next (see [[EmbedBuilder.withNext]]).
     */
    LookerEmbedDashboard.prototype.openScheduleDialog = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, this.sendAndReceive('dashboard:schedule_modal:open')];
            });
        });
    };
    /**
     * Convenience method for loading a new dashboard.
     * Requires Looker 7.12 and Dashboards Next (see [[EmbedBuilder.withNext]]).
     * Throws an error if the dashboard load did not happen, which can happen if the
     * current dashboard is in edit mode.
     *
     * @param id The ID of the dashboard to load
     * @param pushHistory Whether to push the new page onto history. Default is false.
     */
    LookerEmbedDashboard.prototype.loadDashboard = function (id, pushHistory) {
        if (pushHistory === void 0) { pushHistory = false; }
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, this.sendAndReceive('dashboard:load', { id: id, pushHistory: pushHistory })];
            });
        });
    };
    return LookerEmbedDashboard;
}(embed_base_1.LookerEmbedBase));
exports.LookerEmbedDashboard = LookerEmbedDashboard;
//# sourceMappingURL=dashboard_client.js.map