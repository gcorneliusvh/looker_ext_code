import type { IApiSettings } from './apiSettings';
import type { IRequestProps, ITransport } from './transport';
import { AuthSession } from './authSession';
export declare class CSRFSession extends AuthSession {
    settings: IApiSettings;
    _activeToken: string;
    constructor(settings: IApiSettings, transport?: ITransport);
    get activeToken(): string;
    getToken(): Promise<string>;
    isAuthenticated(): boolean;
    authenticate(props: IRequestProps): Promise<IRequestProps>;
}
