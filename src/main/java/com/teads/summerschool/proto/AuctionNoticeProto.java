package com.teads.summerschool.proto;

import com.google.protobuf.CodedInputStream;

import java.io.IOException;

/**
 * Hand-written Protobuf parser for the AuctionNotice message defined in auction_notice.proto.
 * Uses protobuf-java's CodedInputStream to read the wire format — no code generation needed.
 *
 * Wire schema (proto3):
 *   field 1 (string)  request_id
 *   field 2 (double)  clearing_price
 *   field 3 (string)  winning_bidder_id
 *
 * Wire-compatible with the SSP's Python codec (app/proto/codec.py).
 */
public final class AuctionNoticeProto {

    private AuctionNoticeProto() {}

    public static final class AuctionNotice {

        private final String requestId;
        private final double clearingPrice;
        private final String winningBidderId;

        private AuctionNotice(String requestId, double clearingPrice, String winningBidderId) {
            this.requestId = requestId;
            this.clearingPrice = clearingPrice;
            this.winningBidderId = winningBidderId;
        }

        public String getRequestId() { return requestId; }
        public double getClearingPrice() { return clearingPrice; }
        public String getWinningBidderId() { return winningBidderId; }

        public static AuctionNotice parseFrom(byte[] bytes) throws IOException {
            CodedInputStream input = CodedInputStream.newInstance(bytes);
            String requestId = "";
            double clearingPrice = 0.0;
            String winningBidderId = "";

            int tag;
            while ((tag = input.readTag()) != 0) {
                switch (tag >>> 3) {
                    case 1 -> requestId = input.readString();
                    case 2 -> clearingPrice = input.readDouble();
                    case 3 -> winningBidderId = input.readString();
                    default -> input.skipField(tag);
                }
            }
            return new AuctionNotice(requestId, clearingPrice, winningBidderId);
        }
    }
}
