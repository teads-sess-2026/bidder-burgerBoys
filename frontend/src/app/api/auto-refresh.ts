import { DestroyRef, inject } from '@angular/core';

/**
 * Reloads a resource (created with `httpResource`) every `intervalMs` milliseconds,
 * so the page keeps showing fresh data. The timer is stopped automatically when the
 * component is destroyed (e.g. when you navigate to another page).
 *
 * Call this from a component's constructor:
 *   constructor() { autoRefresh(this.stats); }
 */
export function autoRefresh(resource: { reload: () => void }, intervalMs = 5000): void {
    const timer = setInterval(() => resource.reload(), intervalMs);
    inject(DestroyRef).onDestroy(() => clearInterval(timer));
}
