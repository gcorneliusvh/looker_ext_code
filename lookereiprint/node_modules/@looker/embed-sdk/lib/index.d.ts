import { EmbedBuilder } from './embed_builder';
import { LookerEmbedDashboard } from './dashboard_client';
import { LookerEmbedExplore } from './explore_client';
import { LookerEmbedExtension } from './extension_client';
import { LookerEmbedLook } from './look_client';
import type { LookerAuthConfig, CookielessCallback, CookielessRequestInit } from './types';
export type { LookerEmbedDashboard } from './dashboard_client';
export type { LookerEmbedExplore } from './explore_client';
export type { LookerEmbedExtension } from './extension_client';
export type { LookerEmbedLook } from './look_client';
export type { LookerEmbedBase } from './embed_base';
export type { EmbedBuilder, UrlParams } from './embed_builder';
export type { EmbedClient } from './embed';
export * from './types';
export declare class LookerEmbedSDK {
    /**
     * Initialize the Embed SDK.
     *
     * @param apiHost The address or base URL of the Looker host (example.looker.com:9999, https://example.looker.com:9999)
     *                This is required for verification of messages sent from the embedded content.
     * @param authUrl A server endpoint that will sign SSO embed URLs
     */
    static init(apiHost: string, auth?: string | LookerAuthConfig): void;
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
    static initCookieless(apiHost: string, acquireSession: string | CookielessRequestInit | CookielessCallback, generateTokens: string | CookielessRequestInit | CookielessCallback): void;
    /**
     * Create an EmbedBuilder for an embedded Looker dashboard.
     *
     * @param url A signed SSO embed URL or embed URL for an already authenticated Looker user
     */
    static createDashboardWithUrl(url: string): EmbedBuilder<LookerEmbedDashboard>;
    /**
     * Create an EmbedBuilder for an embedded Looker dashboard.
     *
     * @param id The numeric ID of a Looker User Defined Dashboard, or LookML Dashboard ID
     */
    static createDashboardWithId(id: string | number): EmbedBuilder<LookerEmbedDashboard>;
    /**
     * Create an EmbedBuilder for an embedded Looker Explore.
     *
     * @param url A signed SSO embed URL or embed URL for an already authenticated Looker user
     */
    static createExploreWithUrl(url: string): EmbedBuilder<LookerEmbedExplore>;
    /**
     * Create an EmbedBuilder for an embedded Looker Explore.
     *
     * @param id The ID of a Looker explore
     */
    static createExploreWithId(id: string): EmbedBuilder<LookerEmbedExplore>;
    /**
     * Create an EmbedBuilder for an embedded Looker Look.
     *
     * @param url A signed SSO embed URL or embed URL for an already authenticated Looker user
     */
    static createLookWithUrl(url: string): EmbedBuilder<LookerEmbedLook>;
    /**
     * Create an EmbedBuilder for an embedded Looker dashboard.
     *
     * @param id The ID of a Looker Look
     */
    static createLookWithId(id: number): EmbedBuilder<LookerEmbedLook>;
    /**
     * Create an EmbedBuilder for an embedded Looker extension.
     *
     * @param url A signed SSO embed URL or embed URL for an already authenticated Looker user
     */
    static createExtensionWithUrl(url: string): EmbedBuilder<LookerEmbedExtension>;
    /**
     * Create an EmbedBuilder for an embedded Looker extension. Requires Looker 7.12
     *
     * @param id The ID of a Looker Look
     */
    static createExtensionWithId(id: string): EmbedBuilder<LookerEmbedExtension>;
    /**
     * @hidden
     */
    static apiHost: string;
    /**
     * @hidden
     */
    static auth?: LookerAuthConfig;
    /**
     * @hidden
     */
    static acquireSession?: string | CookielessRequestInit | CookielessCallback;
    /**
     * @hidden
     */
    static generateTokens?: string | CookielessRequestInit | CookielessCallback;
}
