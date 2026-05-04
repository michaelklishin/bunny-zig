const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;
const BasicProperties = h.BasicProperties;

test "publish and basic.get" {
    const _t = h.TestTimer.start("publish and basic.get"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);
    try ch.confirmSelect();

    try queue.publish("Hello from bunny-zig!", BasicProperties.persistent);
    try testing.expect(try ch.waitForConfirms());

    const result = try queue.get(.manual);
    try testing.expect(result != null);
    const msg = result.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "Hello from bunny-zig!", msg.body);

    try msg.ack();
}

test "publish and consume with manual ack" {
    const _t = h.TestTimer.start("publish and consume with manual ack"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);
    _ = try queue.subscribeWithTag("test-consumer", .manual);

    try queue.publish("consumed message", .{});

    const delivery = try ch.recvDelivery();
    try testing.expect(delivery != null);
    const msg = delivery.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "consumed message", msg.body);

    try msg.ack();
}

test "publish and consume empty body" {
    const _t = h.TestTimer.start("publish and consume empty body"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);
    try ch.confirmSelect();

    try queue.publish("", .{});
    try testing.expect(try ch.waitForConfirms());

    const got = try h.pollBasicGet(ch, queue.name);
    try testing.expect(got != null);
    const msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqual(0, msg.body.len);

    try msg.ack();
}

test "publish to the default exchange routes by queue name" {
    const _t = h.TestTimer.start("publish to the default exchange routes by queue name"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    // The default exchange is named "" and routes by routing_key == queue name.
    try ch.publish("via default exchange", .{ .exchange = "", .routing_key = queue.name });
    _ = try ch.waitForConfirms();

    const got = try h.pollBasicGet(ch, queue.name);
    try testing.expect(got != null);
    const msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "via default exchange", msg.body);
    try msg.ack();
}

test "publish multiple sequential messages preserves order" {
    const _t = h.TestTimer.start("publish multiple sequential messages preserves order"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var buf: [16]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "m{d}", .{i}) catch unreachable;
        try queue.publish(body, .{});
    }
    _ = try ch.waitForConfirms();

    var seen: u32 = 0;
    while (seen < 5) : (seen += 1) {
        const got = try h.pollBasicGet(ch, queue.name);
        try testing.expect(got != null);
        const m = got.?;
        defer m.deinit(h.test_allocator);
        var expected_buf: [16]u8 = undefined;
        const expected = std.fmt.bufPrint(&expected_buf, "m{d}", .{seen}) catch unreachable;
        try testing.expectEqualSlices(u8, expected, m.body);
        try m.ack();
    }
}

test "publish and consume large message spanning multiple frames" {
    const _t = h.TestTimer.start("publish and consume large message spanning multiple frames"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);
    try ch.confirmSelect();

    // Build a message larger than the negotiated frame_max (typically 131072)
    // to force multi-frame body encoding.
    const body_size = conn.negotiated_frame_max * 2;
    const body = try std.heap.page_allocator.alloc(u8, body_size);
    defer std.heap.page_allocator.free(body);
    @memset(body, 'A');

    try queue.publish(body, .{});
    try testing.expect(try ch.waitForConfirms());

    _ = try queue.subscribe(.manual);
    const delivery = try ch.recvDelivery();
    try testing.expect(delivery != null);
    const d = delivery.?;
    defer d.deinit(h.test_allocator);
    try testing.expectEqual(body_size, d.body.len);
    // Verify first and last bytes survived the multi-frame roundtrip.
    try testing.expectEqual('A', d.body[0]);
    try testing.expectEqual('A', d.body[body_size - 1]);

    try d.ack();
}

test "delivery tags are monotonically increasing within a channel" {
    const _t = h.TestTimer.start("delivery tags are monotonically increasing within a channel");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    const total: u32 = 100;
    for (0..total) |_| {
        try queue.publish("tag", .{});
    }
    _ = try ch.waitForConfirms();

    _ = try queue.subscribe(.manual);

    var prev: u64 = 0;
    var seen: u32 = 0;
    while (seen < total) {
        const got = try ch.recvDelivery();
        try testing.expect(got != null);
        const d = got.?;
        defer d.deinit(h.test_allocator);
        // Delivery tags within a channel are strictly increasing.
        try testing.expect(d.delivery_tag > prev);
        prev = d.delivery_tag;
        try d.ack();
        seen += 1;
    }
    try testing.expectEqual(total, seen);
}
