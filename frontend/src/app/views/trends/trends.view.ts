import { Component } from "@angular/core";
import { CommonModule } from "@angular/common";
import { httpResource } from "@angular/common/http";
import { API_BASE_URL } from "../../app.config";
import { TimeseriesResponse } from "../../api/stats.types";
import { autoRefresh } from "../../api/auto-refresh";

@Component({
    templateUrl: 'trends.view.html',
    styleUrl: 'trends.view.less',
    imports: [CommonModule],
})
export class TrendsView {
    // Loads GET /api/stats/timeseries (defaults: 30-minute window, 60-second buckets).
    timeseries = httpResource<TimeseriesResponse>(() => `${API_BASE_URL}/stats/timeseries`);

    constructor() {
        autoRefresh(this.timeseries);
    }
}
