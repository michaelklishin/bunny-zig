const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "publish with mandatory: unroutable message triggers basic.return" {
    const _t = h.TestTimer.start("publish with mandatory: unroutable message triggers basic.return");
    defer _t.stop();

    const Counter = struct {
        var n: std.atomic.Value(u32) = .init(0);
        fn handler(_: bunny.ReturnedMessage) void {
            _ = n.fetchAdd(1, .release);
        }
    };
    Counter.n.store(0, .release);

    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const ex = "bunny-zig.test.mandatory-fanout";
    try ch.exchangeDeclare(ex, bunny.ExchangeType.fanout, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex) catch {};

    ch.on_return = &Counter.handler;

    try ch.confirmSelect();
    try ch.publish("dropped", .{
        .exchange = ex,
        .routing_key = "no.route",
        .mandatory = true,
    });
    _ = try ch.waitForConfirms();

    var attempts: u32 = 0;
    while (Counter.n.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(20);
    try testing.expectEqual(1, Counter.n.load(.acquire));
}

test "topic exchange routes by wildcard patterns" {
    const _t = h.TestTimer.start("topic exchange routes by wildcard patterns");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const ex = "bunny-zig.test.topic-wildcards";
    try ch.exchangeDeclare(ex, bunny.ExchangeType.topic, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex) catch {};

    const q_star = "bunny-zig.test.topic-wildcards.star";
    const q_hash = "bunny-zig.test.topic-wildcards.hash";
    _ = try ch.queueDeclare(q_star, .{ .exclusive = true, .auto_delete = true });
    _ = try ch.queueDeclare(q_hash, .{ .exclusive = true, .auto_delete = true });

    try ch.queueBind(q_star, ex, "logs.*");
    try ch.queueBind(q_hash, ex, "logs.#");

    try ch.confirmSelect();
    try ch.publish("one-segment", .{ .exchange = ex, .routing_key = "logs.info" });
    try ch.publish("two-segments", .{ .exchange = ex, .routing_key = "logs.app.error" });
    _ = try ch.waitForConfirms();

    // logs.* matches one segment only.
    const star_msg = try h.pollBasicGet(ch, q_star);
    try testing.expect(star_msg != null);
    try testing.expectEqualSlices(u8, "one-segment", star_msg.?.body);
    try ch.basicAck(star_msg.?.delivery_tag, false);
    try testing.expect((try ch.basicGet(q_star, .manual)) == null);

    // logs.# matches both.
    var seen: u32 = 0;
    for (0..20) |_| {
        if (try ch.basicGet(q_hash, .manual)) |m| {
            try ch.basicAck(m.delivery_tag, false);
            seen += 1;
            if (seen == 2) break;
        } else h.sleepMs(25);
    }
    try testing.expectEqual(2, seen);
}
