import type { IRequestProps, ITransport, SDKResponse } from './transport';
import type { IApiSettings } from './apiSettings';
export interface IAccessToken {
    access_token?: string;
    token_type?: string;
    expires_in?: number;
    refresh_token?: string;
}
export interface IError {
    message: string | null;
    documentation_url: string | null;
}
export interface IAuthSession {
    settings: IApiSettings;
    transport: ITransport;
    sudoId: string;
    isAuthenticated(): boolean;
    authenticate(props: IRequestProps): Promise<IRequestProps>;
    logout(): Promise<boolean>;
    getToken(): Promise<any>;
    isSudo(): boolean;
    login(sudoId?: string | number): Promise<any>;
    reset(): void;
}
export declare abstract class AuthSession implements IAuthSession {
    static TBD: string;
    settings: IApiSettings;
    sudoId: string;
    transport: ITransport;
    protected constructor(settings: IApiSettings, transport: ITransport);
    abstract authenticate(props: IRequestProps): Promise<IRequestProps>;
    abstract isAuthenticated(): boolean;
    abstract getToken(): Promise<any>;
    login(_sudoId?: string | number): Promise<any>;
    logout(): Promise<boolean>;
    isSudo(): boolean;
    reset(): void;
    protected ok<TSuccess, TError>(promise: Promise<SDKResponse<TSuccess, TError>>): Promise<TSuccess>;
}
