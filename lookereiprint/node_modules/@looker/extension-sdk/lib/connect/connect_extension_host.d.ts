import type { ExtensionHostApi, ExtensionHostConfiguration } from './types';
export declare const connectExtensionHost: (configuration?: ExtensionHostConfiguration) => Promise<ExtensionHostApi>;
