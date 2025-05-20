import React from 'react';
import type { BaseExtensionContextData, ExtensionProviderProps } from '../ExtensionConnector';
export interface ExtensionContextData2<T> extends BaseExtensionContextData {
    coreSDK: T;
}
export declare const ExtensionContext2: React.Context<ExtensionContextData2<any>>;
export interface ExtensionProvider2Props<T> extends ExtensionProviderProps {
    type: T;
}
export declare function ExtensionProvider2<T>(props: ExtensionProvider2Props<T>): JSX.Element;
