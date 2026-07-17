import { Routes } from '@angular/router';
import { OverviewView } from "./views/overview/overview.view";
import { CreativesView } from "./views/creatives/creatives.view";
import { TargetingView } from "./views/targeting/targeting.view";
import { TrendsView } from "./views/trends/trends.view";
import { AboutView } from "./views/about/about.view";

export const routes: Routes = [
    { path: '', component: OverviewView },
    { path: 'creatives', component: CreativesView },
    { path: 'targeting', component: TargetingView },
    { path: 'trends', component: TrendsView },
    { path: 'about', component: AboutView },
];
