const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "reject and requeue" {
    const _t = h.TestTimer.start("reject and requeue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    _ = try ch.queueDeclare("bunny-zig.test.reject", .{ .exclusive = true, .auto_delete = true });
    try ch.confirmSelect();

    // Use a consumer for reliable delivery
    _ = try ch.basicConsume("bunny-zig.test.reject", .manual);

    try ch.publishToQueue("bunny-zig.test.reject", "rejected message", .{});
    try testing.expect(try ch.waitForConfirms());

    const got1 = try ch.recvDelivery();
    try testing.expect(got1 != null);
    var delivery1 = got1.?;
    defer delivery1.deinit(h.test_allocator);
    try ch.basicReject(delivery1.delivery_tag, true);

    // Wait for redelivered message via consumer.
    const got2 = try ch.recvDelivery();
    try testing.expect(got2 != null);
    var delivery2 = got2.?;
    defer delivery2.deinit(h.test_allocator);
    try testing.expect(delivery2.redelivered);
    try ch.basicAck(delivery2.delivery_tag, false);

    _ = try ch.queueDelete("bunny-zig.test.reject");
}

test "nack with requeue" {
    const _t = h.TestTimer.start("nack with requeue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    _ = try ch.queueDeclare("bunny-zig.test.nack", .{ .exclusive = true, .auto_delete = true });
    try ch.confirmSelect();

    _ = try ch.basicConsume("bunny-zig.test.nack", .manual);

    try ch.publishToQueue("bunny-zig.test.nack", "nacked message", .{});
    try testing.expect(try ch.waitForConfirms());

    const got1 = try ch.recvDelivery();
    try testing.expect(got1 != null);
    var delivery1 = got1.?;
    defer delivery1.deinit(h.test_allocator);
    try ch.basicNack(delivery1.delivery_tag, false, true);

    const got2 = try ch.recvDelivery();
    try testing.expect(got2 != null);
    var delivery2 = got2.?;
    defer delivery2.deinit(h.test_allocator);
    try ch.basicAck(delivery2.delivery_tag, false);

    _ = try ch.queueDelete("bunny-zig.test.nack");
}
