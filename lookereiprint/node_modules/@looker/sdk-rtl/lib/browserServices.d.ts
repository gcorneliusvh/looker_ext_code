import type { ICryptoHash } from './cryptoHash';
import type { IApiSettings } from './apiSettings';
import type { ITransport } from './transport';
import type { IPlatformServices } from './platformServices';
export declare class BrowserServices implements IPlatformServices {
    crypto: ICryptoHash;
    settings: IApiSettings;
    transport: ITransport;
    constructor(services: Partial<IPlatformServices>);
}
