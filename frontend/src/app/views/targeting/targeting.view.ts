import { Component } from "@angular/core";
import { CommonModule } from "@angular/common";
import { httpResource } from "@angular/common/http";
import { API_BASE_URL } from "../../app.config";
import { TargetingResponse } from "../../api/stats.types";
import { autoRefresh } from "../../api/auto-refresh";

@Component({
    templateUrl: 'targeting.view.html',
    styleUrl: 'targeting.view.less',
    imports: [CommonModule],
})
export class TargetingView {
    // Loads GET /api/stats/targeting (dimension=all by default: geo, device and segment).
    targeting = httpResource<TargetingResponse>(() => `${API_BASE_URL}/stats/targeting`);

    constructor() {
        autoRefresh(this.targeting);
    }
}
