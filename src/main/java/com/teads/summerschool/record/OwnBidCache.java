package com.teads.summerschool.record;

import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * In-memory record of this bidder's own recent bids (requestId -> creativeId/bidPrice).
 *
 * <p>AuctionNoticeConsumer.consume() runs on EVERY auction notice broadcast to EVERY
 * bidder, win or loss, whether or not we bid on it — that's most of the topic's volume.
 * Before this cache, the only way to know "did we bid on this one, and if so with what
 * creative/price" was a Postgres lookup by requestId, on every single message. Since we
 * already know that the moment we place a bid (see BiddingService.bid()), caching it in
 * memory turns that per-message DB round trip into an O(1) local lookup, and lets the
 * consumer skip everything else immediately for auctions we never bid on.
 */
@Component
public class OwnBidCache {

    // Bounds memory: auctions are ephemeral, so only recent bids are ever worth keeping —
    // the notice for a given requestId arrives within one auction cycle of the bid.
    private static final int MAX_SIZE = 20_000;

    public record Entry(String creativeId, double bidPrice) {}

    private final ConcurrentHashMap<String, Entry> bids = new ConcurrentHashMap<>();
    private final Queue<String> insertionOrder = new ConcurrentLinkedQueue<>();

    public void record(String requestId, String creativeId, double bidPrice) {
        bids.put(requestId, new Entry(creativeId, bidPrice));
        insertionOrder.add(requestId);
        while (insertionOrder.size() > MAX_SIZE) {
            String oldest = insertionOrder.poll();
            if (oldest != null) bids.remove(oldest);
        }
    }

    /** Our bid on this requestId, or null if we never bid on it (or it's aged out). */
    public Entry get(String requestId) {
        return bids.get(requestId);
    }
}
