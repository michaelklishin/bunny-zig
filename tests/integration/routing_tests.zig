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

    const exchange = try ch.exchangeDeclare(
        "bunny-zig.test.mandatory-with-confirms",
        bunny.ExchangeType.direct,
        .{ .auto_delete = true },
    );
    defer ch.exchangeDelete(exchange.name) catch {};

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);
    try queue.bind(exchange.name, "bound");

    ch.on_return = &Counter.handler;

    try ch.confirmSelect();
    try exchange.publishMandatory("routed-1", "bound", .{});
    try exchange.publishMandatory("dropped", "unbound", .{});
    try exchange.publishMandatory("routed-2", "bound", .{});

    // RabbitMQ confirms returned messages too, so all three are accounted for.
    try testing.expect(try ch.waitForConfirms());

    var attempts: u32 = 0;
    while (Counter.n.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(20);
    try testing.expectEqual(1, Counter.n.load(.acquire));

    var seen: u32 = 0;
    for (0..40) |_| {
        if (try queue.get(.manual)) |m| {
            defer m.deinit(h.test_allocator);
            try m.ack();
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

    const exchange = try ch.exchangeDeclare(
        "bunny-zig.test.mandatory-fanout",
        bunny.ExchangeType.fanout,
        .{ .auto_delete = true },
    );
    defer ch.exchangeDelete(exchange.name) catch {};

    ch.on_return = &Counter.handler;

    try ch.confirmSelect();
    try exchange.publishMandatory("dropped", "no.route", .{});
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

    const exchange = try ch.exchangeDeclare(
        "bunny-zig.test.ssd-direct",
        bunny.ExchangeType.direct,
        .{ .auto_delete = true },
    );
    defer ch.exchangeDelete(exchange.name) catch {};

    var primary = try ch.temporaryQueue();
    defer primary.deinit(h.test_allocator);
    var cc = try ch.temporaryQueue();
    defer cc.deinit(h.test_allocator);
    try primary.bind(exchange.name, "primary");
    try cc.bind(exchange.name, "cc");

    // The CC header carries an array of additional routing keys. The broker
    // routes the message to bindings matching either the primary key or any CC entry.
    const cc_array = [_]bunny.FieldValue{
        .{ .long_string = "cc" },
    };
    var header_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "CC", .value = .{ .array = &cc_array } },
    };

    try ch.confirmSelect();
    try exchange.publish("fanned", "primary", .{
        .headers = .{ .entries = &header_entries, .allocator = undefined },
    });
    _ = try ch.waitForConfirms();

    const got_primary = try h.pollBasicGet(ch, primary.name);
    try testing.expect(got_primary != null);
    const m_primary = got_primary.?;
    defer m_primary.deinit(h.test_allocator);
    try m_primary.ack();

    const got_cc = try h.pollBasicGet(ch, cc.name);
    try testing.expect(got_cc != null);
    const m_cc = got_cc.?;
    defer m_cc.deinit(h.test_allocator);
    try m_cc.ack();
}

test "sender-selected distribution: BCC header routes but is stripped from delivery" {
    const _t = h.TestTimer.start("sender-selected distribution: BCC header routes but is stripped from delivery");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const exchange = try ch.exchangeDeclare(
        "bunny-zig.test.ssd-bcc",
        bunny.ExchangeType.direct,
        .{ .auto_delete = true },
    );
    defer ch.exchangeDelete(exchange.name) catch {};

    var primary = try ch.temporaryQueue();
    defer primary.deinit(h.test_allocator);
    var bcc = try ch.temporaryQueue();
    defer bcc.deinit(h.test_allocator);
    try primary.bind(exchange.name, "primary");
    try bcc.bind(exchange.name, "shadow");

    const bcc_array = [_]bunny.FieldValue{
        .{ .long_string = "shadow" },
    };
    var header_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "BCC", .value = .{ .array = &bcc_array } },
        .{ .key = "trace", .value = .{ .long_string = "keep-me" } },
    };

    try ch.confirmSelect();
    try exchange.publish("fanned", "primary", .{
        .headers = .{ .entries = &header_entries, .allocator = undefined },
    });
    _ = try ch.waitForConfirms();

    const got_primary = try h.pollBasicGet(ch, primary.name);
    try testing.expect(got_primary != null);
    const m_primary = got_primary.?;
    defer m_primary.deinit(h.test_allocator);
    try m_primary.ack();

    const got_bcc = try h.pollBasicGet(ch, bcc.name);
    try testing.expect(got_bcc != null);
    const m_bcc = got_bcc.?;
    defer m_bcc.deinit(h.test_allocator);
    try m_bcc.ack();

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

    const exchange = try ch.declareTopicExchange("bunny-zig.test.topic-wildcards");
    defer ch.exchangeDelete(exchange.name) catch {};

    var q_star = try ch.temporaryQueue();
    defer q_star.deinit(h.test_allocator);
    var q_hash = try ch.temporaryQueue();
    defer q_hash.deinit(h.test_allocator);

    try q_star.bind(exchange.name, "logs.*");
    try q_hash.bind(exchange.name, "logs.#");

    try ch.confirmSelect();
    try exchange.publish("one-segment", "logs.info", .{});
    try exchange.publish("two-segments", "logs.app.error", .{});
    _ = try ch.waitForConfirms();

    // logs.* matches one segment only.
    const got_star = try h.pollBasicGet(ch, q_star.name);
    try testing.expect(got_star != null);
    const star_msg = got_star.?;
    defer star_msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "one-segment", star_msg.body);
    try star_msg.ack();
    try testing.expect((try q_star.get(.manual)) == null);

    // logs.# matches both.
    var seen: u32 = 0;
    for (0..20) |_| {
        if (try q_hash.get(.manual)) |m| {
            defer m.deinit(h.test_allocator);
            try m.ack();
            seen += 1;
            if (seen == 2) break;
        } else h.sleepMs(25);
    }
    try testing.expectEqual(2, seen);
}
