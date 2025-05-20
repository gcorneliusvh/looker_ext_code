import type { Readable } from 'readable-stream';
import type { HttpMethod, Values, Authenticator, ITransportSettings, IHostConnection } from '@looker/sdk-rtl';
import type { ExtensionHostApi, ApiVersion } from '../connect';
export declare class SdkConnection implements IHostConnection {
    private hostConnection;
    private apiVersion;
    constructor(hostConnection: ExtensionHostApi, apiVersion: ApiVersion);
    request(httpMethod: string, path: string, body?: any, params?: any, _authenticator?: any, options?: any): Promise<any>;
    rawRequest(httpMethod: string, path: string, body?: any, params?: any, _authenticator?: Authenticator, _options?: Partial<ITransportSettings>): Promise<any>;
    stream<T>(_callback: (readable: Readable) => Promise<T>, _method: HttpMethod, _path: string, _queryParams?: Values, _body?: any, _authenticator?: Authenticator, _options?: Partial<ITransportSettings>): Promise<any>;
}
