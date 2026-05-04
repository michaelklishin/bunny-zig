const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "publish with mandatory: returned and routable messages are both confirmed" {
    const _t = h.TestTimer.start("publish with mandatory: returned and routable messages are both confirmed");
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
    defer ch.close();

    const ex = "bunny-zig.test.mandatory-with-confirms";
    _ = try ch.exchangeDeclare(ex, bunny.ExchangeType.direct, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex) catch {};

    const q = "bunny-zig.test.mandatory-with-confirms.q";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });
    try ch.queueBind(q, ex, "bound");

    ch.on_return = &Counter.handler;

    try ch.confirmSelect();
    try ch.publish("routed-1", .{ .exchange = ex, .routing_key = "bound", .mandatory = true });
    try ch.publish("dropped", .{ .exchange = ex, .routing_key = "unbound", .mandatory = true });
    try ch.publish("routed-2", .{ .exchange = ex, .routing_key = "bound", .mandatory = true });

    // RabbitMQ confirms returned messages too, so all three are accounted for.
    try testing.expect(try ch.waitForConfirms());

    var attempts: u32 = 0;
    while (Counter.n.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(20);
    try testing.expectEqual(1, Counter.n.load(.acquire));

    var seen: u32 = 0;
    for (0..40) |_| {
        if (try ch.basicGet(q, .manual)) |raw| {
            var m = raw;
            defer m.deinit(h.test_allocator);
            try ch.basicAck(m.delivery_tag, false);
            seen += 1;
            if (seen == 2) break;
        } else h.sleepMs(25);
    }
    try testing.expectEqual(2, seen);
}

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
    defer ch.close();

    const ex = "bunny-zig.test.mandatory-fanout";
    _ = try ch.exchangeDeclare(ex, bunny.ExchangeType.fanout, .{ .auto_delete = true });
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

test "sender-selected distribution: CC header adds extra routing keys" {
    const _t = h.TestTimer.start("sender-selected distribution: CC header adds extra routing keys"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const ex = "bunny-zig.test.ssd-direct";
    _ = try ch.exchangeDeclare(ex, bunny.ExchangeType.direct, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex) catch {};

    const q_primary = "bunny-zig.test.ssd.primary";
    const q_cc = "bunny-zig.test.ssd.cc";
    _ = try ch.queueDeclare(q_primary, .{ .exclusive = true, .auto_delete = true });
    _ = try ch.queueDeclare(q_cc, .{ .exclusive = true, .auto_delete = true });
    try ch.queueBind(q_primary, ex, "primary");
    try ch.queueBind(q_cc, ex, "cc");

    // The CC header carries an array of additional routing keys. The broker
    // routes the message to bindings matching either the primary key or any CC entry.
    const cc_array = [_]bunny.FieldValue{
        .{ .long_string = "cc" },
    };
    var header_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "CC", .value = .{ .array = &cc_array } },
    };

    try ch.confirmSelect();
    try ch.publish("fanned", .{
        .exchange = ex,
        .routing_key = "primary",
        .properties = .{ .headers = .{ .entries = &header_entries, .allocator = undefined } },
    });
    _ = try ch.waitForConfirms();

    const got_primary = try h.pollBasicGet(ch, q_primary);
    try testing.expect(got_primary != null);
    var m_primary = got_primary.?;
    defer m_primary.deinit(h.test_allocator);
    try ch.basicAck(m_primary.delivery_tag, false);

    const got_cc = try h.pollBasicGet(ch, q_cc);
    try testing.expect(got_cc != null);
    var m_cc = got_cc.?;
    defer m_cc.deinit(h.test_allocator);
    try ch.basicAck(m_cc.delivery_tag, false);
}

test "sender-selected distribution: BCC header routes but is stripped from delivery" {
    const _t = h.TestTimer.start("sender-selected distribution: BCC header routes but is stripped from delivery");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const ex = "bunny-zig.test.ssd-bcc";
    _ = try ch.exchangeDeclare(ex, bunny.ExchangeType.direct, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex) catch {};

    const q_primary = "bunny-zig.test.ssd-bcc.primary";
    const q_bcc = "bunny-zig.test.ssd-bcc.bcc";
    _ = try ch.queueDeclare(q_primary, .{ .exclusive = true, .auto_delete = true });
    _ = try ch.queueDeclare(q_bcc, .{ .exclusive = true, .auto_delete = true });
    try ch.queueBind(q_primary, ex, "primary");
    try ch.queueBind(q_bcc, ex, "shadow");

    const bcc_array = [_]bunny.FieldValue{
        .{ .long_string = "shadow" },
    };
    var header_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "BCC", .value = .{ .array = &bcc_array } },
        .{ .key = "trace", .value = .{ .long_string = "keep-me" } },
    };

    try ch.confirmSelect();
    try ch.publish("fanned", .{
        .exchange = ex,
        .routing_key = "primary",
        .properties = .{ .headers = .{ .entries = &header_entries, .allocator = undefined } },
    });
    _ = try ch.waitForConfirms();

    // Both queues receive the message.
    const got_primary = try h.pollBasicGet(ch, q_primary);
    try testing.expect(got_primary != null);
    var m_primary = got_primary.?;
    defer m_primary.deinit(h.test_allocator);
    try ch.basicAck(m_primary.delivery_tag, false);

    const got_bcc = try h.pollBasicGet(ch, q_bcc);
    try testing.expect(got_bcc != null);
    var m_bcc = got_bcc.?;
    defer m_bcc.deinit(h.test_allocator);
    try ch.basicAck(m_bcc.delivery_tag, false);

    // The BCC header is stripped from the delivered headers; other headers survive.
    if (m_bcc.properties.headers) |delivered| {
        var saw_bcc = false;
        var saw_trace = false;
        for (delivered.entries) |entry| {
            if (std.mem.eql(u8, entry.key, "BCC")) saw_bcc = true;
            if (std.mem.eql(u8, entry.key, "trace")) saw_trace = true;
        }
        try testing.expect(!saw_bcc);
        try testing.expect(saw_trace);
    }
}

test "topic exchange routes by wildcard patterns" {
    const _t = h.TestTimer.start("topic exchange routes by wildcard patterns");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const ex = "bunny-zig.test.topic-wildcards";
    _ = try ch.exchangeDeclare(ex, bunny.ExchangeType.topic, .{ .auto_delete = true });
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
    const got_star = try h.pollBasicGet(ch, q_star);
    try testing.expect(got_star != null);
    var star_msg = got_star.?;
    defer star_msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "one-segment", star_msg.body);
    try ch.basicAck(star_msg.delivery_tag, false);
    try testing.expect((try ch.basicGet(q_star, .manual)) == null);

    // logs.# matches both.
    var seen: u32 = 0;
    for (0..20) |_| {
        if (try ch.basicGet(q_hash, .manual)) |raw| {
            var m = raw;
            defer m.deinit(h.test_allocator);
            try ch.basicAck(m.delivery_tag, false);
            seen += 1;
            if (seen == 2) break;
        } else h.sleepMs(25);
    }
    try testing.expectEqual(2, seen);
}
