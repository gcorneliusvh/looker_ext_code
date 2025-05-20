import type { ExtensionSDK } from '@looker/extension-sdk';
export interface RouteChangeListenerProps {
    onPathnameChange?: (pathname: string) => void;
    onRouteChange?: (route: string, routeState?: any) => void;
    extensionHost?: ExtensionSDK;
    hostRoute?: string;
    hostRouteState?: any;
}
