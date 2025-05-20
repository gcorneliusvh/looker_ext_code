import type { FetchProxy, FetchCustomParameters, FetchResponseBodyType, FetchProxyDataResponse } from './types';
import type { ExtensionHostApiImpl } from './extension_host_api';
export declare class FetchProxyImpl implements FetchProxy {
    private extensionHost;
    private baseUrl?;
    private init?;
    private responseBodyType?;
    constructor(extensionHost: ExtensionHostApiImpl, baseUrl?: string | undefined, init?: FetchCustomParameters | undefined, responseBodyType?: FetchResponseBodyType | undefined);
    fetchProxy(resource: string, init?: FetchCustomParameters, responseBodyType?: FetchResponseBodyType): Promise<FetchProxyDataResponse>;
    private getResource;
    private getInit;
    private getResponseBodyType;
}
