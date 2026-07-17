import { Component } from "@angular/core";
import { CommonModule } from "@angular/common";
import { httpResource } from "@angular/common/http";
import { API_BASE_URL } from "../../app.config";
import { Stats } from "../../api/stats.types";
import { autoRefresh } from "../../api/auto-refresh";

@Component({
    templateUrl: 'overview.view.html',
    styleUrl: 'overview.view.less',
    imports: [CommonModule],
})
export class OverviewView {
    // Loads GET /api/stats. `stats.value()` holds the response once it arrives.
    stats = httpResource<Stats>(() => `${API_BASE_URL}/stats`);

    constructor() {
        autoRefresh(this.stats);
    }
}
