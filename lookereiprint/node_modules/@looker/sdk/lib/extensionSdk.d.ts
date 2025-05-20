import type { IApiSettings, APIMethods, IAuthSession, IHostConnection } from '@looker/sdk-rtl';
export declare class LookerExtensionSDK {
    static createClient<T extends APIMethods>(hostConnection: IHostConnection, type: new (authSession: IAuthSession) => T, settings?: IApiSettings): T;
}
