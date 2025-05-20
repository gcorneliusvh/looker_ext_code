import type { IApiSettings } from './apiSettings';
import type { ITransport } from './transport';
import type { ICryptoHash } from './cryptoHash';
export interface IPlatformServices {
    settings: IApiSettings;
    transport: ITransport;
    crypto: ICryptoHash;
}
