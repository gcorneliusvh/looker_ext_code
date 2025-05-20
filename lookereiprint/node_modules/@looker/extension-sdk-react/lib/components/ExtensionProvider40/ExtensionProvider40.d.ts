import React from 'react';
import type { ILooker40SDK } from '@looker/sdk';
import type { BaseExtensionContextData, ExtensionProviderProps } from '../ExtensionConnector';
export interface ExtensionContextData40 extends BaseExtensionContextData {
    coreSDK: ILooker40SDK;
}
export declare const ExtensionContext40: React.Context<ExtensionContextData40>;
export interface ExtensionProvider40Props extends ExtensionProviderProps {
}
export declare function ExtensionProvider40(props: ExtensionProvider40Props): JSX.Element;
