const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;
const BasicProperties = h.BasicProperties;

test "publish and basic.get" {
    const _t = h.TestTimer.start("publish and basic.get"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.basic-get", .{ .exclusive = true, .auto_delete = true });
    try ch.confirmSelect();

    try ch.publishToQueue("bunny-zig.test.basic-get", "Hello from bunny-zig!", BasicProperties.persistent);
    try testing.expect(try ch.waitForConfirms());

    const result = try ch.basicGet("bunny-zig.test.basic-get", .manual);
    try testing.expect(result != null);
    var msg = result.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "Hello from bunny-zig!", msg.body);

    try ch.basicAck(msg.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.basic-get");
}

test "publish and consume with manual ack" {
    const _t = h.TestTimer.start("publish and consume with manual ack"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.consume", .{ .exclusive = true, .auto_delete = true });
    _ = try ch.basicConsume("bunny-zig.test.consume", "test-consumer", .manual);

    try ch.publishToQueue("bunny-zig.test.consume", "consumed message", .{});

    const delivery = try ch.recvDelivery();
    try testing.expect(delivery != null);
    var msg = delivery.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "consumed message", msg.body);

    try ch.basicAck(msg.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.consume");
}

test "publish and consume empty body" {
    const _t = h.TestTimer.start("publish and consume empty body"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.empty-body", .{ .exclusive = true, .auto_delete = true });
    try ch.confirmSelect();

    try ch.publishToQueue("bunny-zig.test.empty-body", "", .{});
    try testing.expect(try ch.waitForConfirms());

    const got = try h.pollBasicGet(ch, "bunny-zig.test.empty-body");
    try testing.expect(got != null);
    var msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqual(0, msg.body.len);

    try ch.basicAck(msg.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.empty-body");
}

test "publish to the default exchange routes by queue name" {
    const _t = h.TestTimer.start("publish to the default exchange routes by queue name"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.default-exchange";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    // The default exchange is named "" and routes by routing_key == queue name.
    try ch.publish("via default exchange", .{ .exchange = "", .routing_key = q });
    _ = try ch.waitForConfirms();

    const got = try h.pollBasicGet(ch, q);
    try testing.expect(got != null);
    var msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "via default exchange", msg.body);
    try ch.basicAck(msg.delivery_tag, false);
}

test "publish multiple sequential messages preserves order" {
    const _t = h.TestTimer.start("publish multiple sequential messages preserves order"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.fifo";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var buf: [16]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "m{d}", .{i}) catch unreachable;
        try ch.publishToQueue(q, body, .{});
    }
    _ = try ch.waitForConfirms();

    var seen: u32 = 0;
    while (seen < 5) : (seen += 1) {
        const got = try h.pollBasicGet(ch, q);
        try testing.expect(got != null);
        var m = got.?;
        defer m.deinit(h.test_allocator);
        var expected_buf: [16]u8 = undefined;
        const expected = std.fmt.bufPrint(&expected_buf, "m{d}", .{seen}) catch unreachable;
        try testing.expectEqualSlices(u8, expected, m.body);
        try ch.basicAck(m.delivery_tag, false);
    }
}

test "publish and consume large message spanning multiple frames" {
    const _t = h.TestTimer.start("publish and consume large message spanning multiple frames"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.large-msg", .{ .exclusive = true, .auto_delete = true });
    try ch.confirmSelect();

    // Build a message larger than the negotiated frame_max (typically 131072).
    // This forces multi-frame body encoding.
    const body_size = conn.negotiated_frame_max * 2;
    const body = try std.heap.page_allocator.alloc(u8, body_size);
    defer std.heap.page_allocator.free(body);
    @memset(body, 'A');

    try ch.publishToQueue("bunny-zig.test.large-msg", body, .{});
    try testing.expect(try ch.waitForConfirms());

    _ = try ch.basicConsume("bunny-zig.test.large-msg", "", .manual);
    const delivery = try ch.recvDelivery();
    try testing.expect(delivery != null);
    var d = delivery.?;
    defer d.deinit(h.test_allocator);
    try testing.expectEqual(body_size, d.body.len);
    // Verify first and last bytes survived the multi-frame roundtrip.
    try testing.expectEqual('A', d.body[0]);
    try testing.expectEqual('A', d.body[body_size - 1]);

    try ch.basicAck(d.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.large-msg");
}

test "delivery tags are monotonically increasing within a channel" {
    const _t = h.TestTimer.start("delivery tags are monotonically increasing within a channel");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.delivery-tags";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    const total: u32 = 100;
    for (0..total) |_| {
        try ch.publishToQueue(q, "tag", .{});
    }
    _ = try ch.waitForConfirms();

    _ = try ch.basicConsume(q, "", .manual);

    var prev: u64 = 0;
    var seen: u32 = 0;
    while (seen < total) {
        const got = try ch.recvDelivery();
        try testing.expect(got != null);
        var d = got.?;
        defer d.deinit(h.test_allocator);
        // Delivery tags within a channel are strictly increasing.
        try testing.expect(d.delivery_tag > prev);
        prev = d.delivery_tag;
        try ch.basicAck(d.delivery_tag, false);
        seen += 1;
    }
    try testing.expectEqual(total, seen);
}
