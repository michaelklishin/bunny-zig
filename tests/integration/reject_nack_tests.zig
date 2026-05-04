const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "reject and requeue" {
    const _t = h.TestTimer.start("reject and requeue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);
    try ch.confirmSelect();

    _ = try queue.subscribe(.manual);

    try queue.publish("rejected message", .{});
    try testing.expect(try ch.waitForConfirms());

    const got1 = try ch.recvDelivery();
    try testing.expect(got1 != null);
    const delivery1 = got1.?;
    defer delivery1.deinit(h.test_allocator);
    try delivery1.rejectRequeue();

    // Wait for the requeued message to come back via the consumer.
    const got2 = try ch.recvDelivery();
    try testing.expect(got2 != null);
    const delivery2 = got2.?;
    defer delivery2.deinit(h.test_allocator);
    try testing.expect(delivery2.redelivered);
    try delivery2.ack();
}

test "nack with requeue" {
    const _t = h.TestTimer.start("nack with requeue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);
    try ch.confirmSelect();

    _ = try queue.subscribe(.manual);

    try queue.publish("nacked message", .{});
    try testing.expect(try ch.waitForConfirms());

    const got1 = try ch.recvDelivery();
    try testing.expect(got1 != null);
    const delivery1 = got1.?;
    defer delivery1.deinit(h.test_allocator);
    try delivery1.nackRequeue();

    const got2 = try ch.recvDelivery();
    try testing.expect(got2 != null);
    const delivery2 = got2.?;
    defer delivery2.deinit(h.test_allocator);
    try delivery2.ack();
}
