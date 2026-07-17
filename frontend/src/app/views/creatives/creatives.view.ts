import { Component, signal } from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { httpResource } from "@angular/common/http";
import { API_BASE_URL } from "../../app.config";
import { CreativesResponse } from "../../api/stats.types";
import { autoRefresh } from "../../api/auto-refresh";

@Component({
    templateUrl: 'creatives.view.html',
    styleUrl: 'creatives.view.less',
    imports: [CommonModule, FormsModule],
})
export class CreativesView {
    // These signals are bound to the dropdowns below.
    sort = signal('spend');
    order = signal('desc');

    // Loads GET /api/stats/creatives. Because the URL reads the signals above,
    // changing a dropdown automatically re-fetches with the new sort/order.
    creatives = httpResource<CreativesResponse>(() =>
        `${API_BASE_URL}/stats/creatives?sort=${this.sort()}&order=${this.order()}`
    );

    constructor() {
        autoRefresh(this.creatives);
    }
}
