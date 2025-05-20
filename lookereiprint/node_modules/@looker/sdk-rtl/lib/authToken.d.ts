import type { IAccessToken } from './authSession';
export declare class AuthToken implements IAccessToken {
    lagTime: number;
    access_token: string;
    token_type: string;
    refresh_token?: string;
    expires_in: number;
    expiresAt: Date;
    constructor(token?: IAccessToken);
    isActive(): boolean;
    setToken(token: IAccessToken): this;
    reset(): void;
}
