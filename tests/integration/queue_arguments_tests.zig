const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "queue with x-message-ttl expires messages" {
    const _t = h.TestTimer.start("queue with x-message-ttl expires messages"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    var qa = bunny.QueueArguments{};
    defer qa.deinit(h.test_allocator);
    _ = try qa.messageTtl(h.test_allocator, 50);
    var args = try qa.build(h.test_allocator);
    defer args.deinit();

    const q = "bunny-zig.test.queue-ttl";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true, .arguments = args });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "ephemeral", .{});
    _ = try ch.waitForConfirms();

    h.sleepMs(300);
    const msg = try ch.basicGet(q, .manual);
    try testing.expect(msg == null);
}

test "queue with x-max-length drops oldest messages" {
    const _t = h.TestTimer.start("queue with x-max-length drops oldest messages"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    var qa = bunny.QueueArguments{};
    defer qa.deinit(h.test_allocator);
    _ = try qa.maxLength(h.test_allocator, 2);
    var args = try qa.build(h.test_allocator);
    defer args.deinit();

    const q = "bunny-zig.test.queue-max-length";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true, .arguments = args });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "first", .{});
    try ch.publishToQueue(q, "second", .{});
    try ch.publishToQueue(q, "third", .{});
    _ = try ch.waitForConfirms();

    // Default overflow strategy is drop-head, so "first" is evicted.
    const got_m1 = try h.pollBasicGet(ch, q);
    try testing.expect(got_m1 != null);
    var m1 = got_m1.?;
    defer m1.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "second", m1.body);
    try ch.basicAck(m1.delivery_tag, false);

    const got_m2 = try h.pollBasicGet(ch, q);
    try testing.expect(got_m2 != null);
    var m2 = got_m2.?;
    defer m2.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "third", m2.body);
    try ch.basicAck(m2.delivery_tag, false);

    try testing.expect((try ch.basicGet(q, .manual)) == null);
}

test "queue with x-max-priority delivers higher priority first" {
    const _t = h.TestTimer.start("queue with x-max-priority delivers higher priority first"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    var qa = bunny.QueueArguments{};
    defer qa.deinit(h.test_allocator);
    _ = try qa.maxPriority(h.test_allocator, 10);
    var args = try qa.build(h.test_allocator);
    defer args.deinit();

    const q = "bunny-zig.test.priority-queue";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true, .arguments = args });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "low", .{ .priority = 1 });
    try ch.publishToQueue(q, "high", .{ .priority = 9 });
    _ = try ch.waitForConfirms();

    const got_first = try h.pollBasicGet(ch, q);
    try testing.expect(got_first != null);
    var first = got_first.?;
    defer first.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "high", first.body);
    try ch.basicAck(first.delivery_tag, false);

    const got_second = try h.pollBasicGet(ch, q);
    try testing.expect(got_second != null);
    var second = got_second.?;
    defer second.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "low", second.body);
    try ch.basicAck(second.delivery_tag, false);
}

test "queue with x-expires auto-deletes after the timeout" {
    const _t = h.TestTimer.start("queue with x-expires auto-deletes after the timeout"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    var qa = bunny.QueueArguments{};
    defer qa.deinit(h.test_allocator);
    _ = try qa.expires(h.test_allocator, 200);
    var args = try qa.build(h.test_allocator);
    defer args.deinit();

    const q = "bunny-zig.test.expiring-queue";
    _ = try ch.queueDeclare(q, .{ .durable = true, .arguments = args });

    h.sleepMs(800);

    // After the queue has expired, a passive redeclare must fail with a
    // channel-level NOT_FOUND. The channel reports this as ChannelClosed.
    const result = ch.queueDeclare(q, .{ .passive = true });
    try testing.expectError(error.ChannelClosed, result);
}
