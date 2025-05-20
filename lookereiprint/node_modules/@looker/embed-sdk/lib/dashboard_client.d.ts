import type { LookerDashboardOptions, LookerEmbedFilterParams } from './types';
import { LookerEmbedBase } from './embed_base';
/**
 * Client that communicates with an embedded Looker dashboard. Messages are documented
 * [here](https://docs.looker.com/r/sdk/events)
 */
export declare class LookerEmbedDashboard extends LookerEmbedBase {
    /**
     * Convenience method for sending a run message to the embedded dashboard.
     */
    run(): void;
    /**
     * Convenience method for sending a stop message to the embedded dashboard.
     *
     * Requires Looker 7.14 and Dashboards Next (see [[EmbedBuilder.withNext]]).
     */
    stop(): void;
    /**
     * Convenience method for sending an edit message to the embedded dashboard.
     *
     * Requires Looker 22.20 and Dashboards Next (see [[EmbedBuilder.withNext]]).
     */
    edit(): void;
    /**
     * Convenience method for updating the filters of the embedded dashboard.
     *
     * @param filters A set of filter parameters to update
     */
    updateFilters(params: LookerEmbedFilterParams): void;
    /**
     * Convenience method for setting options on the embedded dashboard.
     *
     * @param options An options object to be applied
     */
    setOptions(options: LookerDashboardOptions): void;
    /**
     * Convenience method for opening the dashboard schedule dialog.
     *
     * Requires Looker 7.18 and Dashboards Next (see [[EmbedBuilder.withNext]]).
     */
    openScheduleDialog(): Promise<void>;
    /**
     * Convenience method for loading a new dashboard.
     * Requires Looker 7.12 and Dashboards Next (see [[EmbedBuilder.withNext]]).
     * Throws an error if the dashboard load did not happen, which can happen if the
     * current dashboard is in edit mode.
     *
     * @param id The ID of the dashboard to load
     * @param pushHistory Whether to push the new page onto history. Default is false.
     */
    loadDashboard(id: string, pushHistory?: boolean): Promise<void>;
}
