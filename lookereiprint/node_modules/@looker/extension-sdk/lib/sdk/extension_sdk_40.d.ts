import { Looker40SDK } from '@looker/sdk';
import type { ExtensionHostApi } from '../connect';
export declare class LookerExtensionSDK40 {
    static createClient(hostConnection: ExtensionHostApi): Looker40SDK;
}
