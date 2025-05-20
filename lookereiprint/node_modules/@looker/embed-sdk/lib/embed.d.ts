import type { ChattyHost, ChattyHostBuilder } from '@looker/chatty';
import type { EmbedBuilder } from './embed_builder';
/**
 * Wrapper for Looker embedded content. Provides a mechanism for creating the embedded content element,
 * and for establishing two-way communication between the parent window and the embedded content.
 */
export declare class EmbedClient<T> {
    private _builder;
    private static sessionAcquired;
    private static acquireSessionPromise?;
    _hostBuilder: ChattyHostBuilder | null;
    _host: ChattyHost | null;
    _connection: Promise<T> | null;
    _client: T | null;
    _cookielessInitialized: boolean;
    _cookielessApiToken?: string | null;
    _cookielessApiTokenTtl?: number | null;
    _cookielessNavigationToken?: string | null;
    _cookielessNavigationTokenTtl?: number | null;
    _cookielessSessionReferenceTokenTtl?: number | null;
    /**
     * @hidden
     */
    constructor(_builder: EmbedBuilder<T>);
    /**
     * Returns a promise that resolves to a client that can be used to send messages to the
     * embedded content.
     */
    get connection(): Promise<T> | null;
    /**
     * Indicates whether two way communication has successfully been established with the embedded content.
     */
    get isConnected(): boolean;
    get targetOrigin(): string;
    private createIframe;
    private sendScrollData;
    private addIframeMonitor;
    private createUrl;
    private acquireCookielessEmbedSession;
    private acquireCookielessEmbedSessionInternal;
    private acquireSession;
    private generateTokens;
    private getResource;
    /**
     * Establish two way communication with embedded content. Returns a promise that resolves to a
     * client that can be used to send messages to the embedded content.
     */
    connect(): Promise<T>;
}
