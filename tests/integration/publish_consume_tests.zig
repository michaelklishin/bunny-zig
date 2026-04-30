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
    const msg = result.?;
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
    const msg = delivery.?;
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

    const msg = try h.pollBasicGet(ch, "bunny-zig.test.empty-body");
    try testing.expect(msg != null);
    try testing.expectEqual(0, msg.?.body.len);

    try ch.basicAck(msg.?.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.empty-body");
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
    try testing.expectEqual(body_size, delivery.?.body.len);
    // Verify first and last bytes survived the multi-frame roundtrip
    try testing.expectEqual('A', delivery.?.body[0]);
    try testing.expectEqual('A', delivery.?.body[body_size - 1]);

    try ch.basicAck(delivery.?.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.large-msg");
}
