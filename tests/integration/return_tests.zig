const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// basic.return delivers the full message back to the publisher with the
// original exchange, routing key, and a broker-supplied reply code/text.

// `Captured` uses static globals: relies on Zig's serial test execution.
const Captured = struct {
    var count: std.atomic.Value(u32) = .init(0);
    var reply_code: u16 = 0;
    var reply_text_buf: [128]u8 = undefined;
    var reply_text_len: usize = 0;
    var exchange_buf: [128]u8 = undefined;
    var exchange_len: usize = 0;
    var routing_key_buf: [128]u8 = undefined;
    var routing_key_len: usize = 0;
    var body_buf: [256]u8 = undefined;
    var body_len: usize = 0;

    fn handler(r: bunny.ReturnedMessage) void {
        reply_code = r.reply_code;
        reply_text_len = @min(reply_text_buf.len, r.reply_text.len);
        @memcpy(reply_text_buf[0..reply_text_len], r.reply_text[0..reply_text_len]);
        exchange_len = @min(exchange_buf.len, r.exchange.len);
        @memcpy(exchange_buf[0..exchange_len], r.exchange[0..exchange_len]);
        routing_key_len = @min(routing_key_buf.len, r.routing_key.len);
        @memcpy(routing_key_buf[0..routing_key_len], r.routing_key[0..routing_key_len]);
        body_len = @min(body_buf.len, r.body.len);
        @memcpy(body_buf[0..body_len], r.body[0..body_len]);
        _ = count.fetchAdd(1, .release);
    }

    fn reset() void {
        count.store(0, .release);
        reply_code = 0;
        reply_text_len = 0;
        exchange_len = 0;
        routing_key_len = 0;
        body_len = 0;
    }
};

test "basic.return: mandatory unroutable message reports exchange, routing_key, and 312 NO_ROUTE" {
    const _t = h.TestTimer.start("basic.return: mandatory unroutable message reports exchange, routing_key, and 312 NO_ROUTE");
    defer _t.stop();
    Captured.reset();

    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();
    ch.on_return = &Captured.handler;

    try ch.confirmSelect();
    try ch.publish("orphan", .{
        .exchange = "amq.direct",
        .routing_key = "no.such.binding",
        .mandatory = true,
    });
    _ = try ch.waitForConfirms();

    var attempts: u32 = 0;
    while (Captured.count.load(.acquire) == 0 and attempts < 50) : (attempts += 1) h.sleepMs(20);

    try testing.expectEqual(@as(u32, 1), Captured.count.load(.acquire));
    try testing.expectEqual(@as(u16, 312), Captured.reply_code);
    try testing.expectEqualSlices(u8, "amq.direct", Captured.exchange_buf[0..Captured.exchange_len]);
    try testing.expectEqualSlices(u8, "no.such.binding", Captured.routing_key_buf[0..Captured.routing_key_len]);
    try testing.expectEqualSlices(u8, "orphan", Captured.body_buf[0..Captured.body_len]);
}

test "basic.return: multiple unroutable mandatory publishes each return separately" {
    const _t = h.TestTimer.start("basic.return: multiple unroutable mandatory publishes each return separately");
    defer _t.stop();
    Captured.reset();

    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();
    ch.on_return = &Captured.handler;

    try ch.confirmSelect();
    for (0..3) |i| {
        var buf: [32]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "msg-{d}", .{i}) catch "msg";
        try ch.publish(body, .{
            .exchange = "amq.topic",
            .routing_key = "no.matching.binding",
            .mandatory = true,
        });
    }
    _ = try ch.waitForConfirms();

    var attempts: u32 = 0;
    while (Captured.count.load(.acquire) < 3 and attempts < 100) : (attempts += 1) h.sleepMs(20);
    try testing.expectEqual(@as(u32, 3), Captured.count.load(.acquire));
}
