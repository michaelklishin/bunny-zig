const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "declare and delete a queue" {
    const _t = h.TestTimer.start("declare and delete a queue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.queueDeclare("bunny-zig.test.declare-delete", .{ .exclusive = true, .auto_delete = true });
    try testing.expectEqualSlices(u8, "bunny-zig.test.declare-delete", info.name);

    _ = try ch.queueDelete("bunny-zig.test.declare-delete");
}

test "declare a durable queue" {
    const _t = h.TestTimer.start("declare a durable queue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.durableQueue("bunny-zig.test.durable");
    try testing.expectEqualSlices(u8, "bunny-zig.test.durable", info.name);

    _ = try ch.queueDelete("bunny-zig.test.durable");
}

test "declare a temporary queue" {
    const _t = h.TestTimer.start("declare a temporary queue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.temporaryQueue();
    try testing.expect(info.name.len > 0);
}

test "queue purge" {
    const _t = h.TestTimer.start("queue purge"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.purge", .{ .exclusive = true, .auto_delete = true });
    try ch.confirmSelect();

    for (0..5) |_| {
        try ch.publishToQueue("bunny-zig.test.purge", "test message", .{});
    }

    const confirmed = try ch.waitForConfirms();
    try testing.expect(confirmed);

    const purged = try ch.queuePurge("bunny-zig.test.purge");
    try testing.expect(purged >= 5);
    _ = try ch.queueDelete("bunny-zig.test.purge");
}

test "server-named queue: empty name returns a generated name" {
    const _t = h.TestTimer.start("server-named queue: empty name returns a generated name");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.queueDeclare("", .{ .exclusive = true, .auto_delete = true });
    try testing.expect(info.name.len > 0);
    try testing.expect(std.mem.startsWith(u8, info.name, "amq."));
}

test "passive declare of an existing queue returns its info" {
    const _t = h.TestTimer.start("passive declare of an existing queue returns its info"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.passive-existing";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "hello", .{});
    _ = try ch.waitForConfirms();

    const info = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqualSlices(u8, q, info.name);
    try testing.expect(info.message_count >= 1);
}

test "passive declare of a missing queue closes the channel" {
    const _t = h.TestTimer.start("passive declare of a missing queue closes the channel"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const result = ch.queueDeclare("bunny-zig.test.passive-missing", .{ .passive = true });
    try testing.expectError(error.ChannelClosed, result);
    try testing.expect(!ch.isOpen());
}

test "redeclaring a queue with mismatched durable raises a precondition error" {
    const _t = h.TestTimer.start("redeclaring a queue with mismatched durable raises a precondition error");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    const q = "bunny-zig.test.redeclare-mismatch";

    const ch1 = try conn.openChannel();
    _ = try ch1.queueDeclare(q, .{ .durable = true });

    const ch2 = try conn.openChannel();
    defer ch2.closeChannel() catch {};
    const result = ch2.queueDeclare(q, .{ .durable = false });
    try testing.expectError(error.ChannelClosed, result);
    try testing.expect(!ch2.isOpen());

    // The failed declare causes a channel exception that closed ch2, so use
    // the original channel to clean up the durable queue we created above.
    _ = try ch1.queueDelete(q);
    try ch1.closeChannel();
}

test "declare and use a quorum queue" {
    const _t = h.TestTimer.start("declare and use a quorum queue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const qi = try ch.quorumQueue("bunny-zig.test.quorum");
    try testing.expect(qi.message_count == 0);

    try ch.confirmSelect();
    try ch.publishToQueue("bunny-zig.test.quorum", "quorum message", .{});
    try testing.expect(try ch.waitForConfirms());

    const got = try h.pollBasicGet(ch, "bunny-zig.test.quorum");
    try testing.expect(got != null);
    var msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "quorum message", msg.body);
    try ch.basicAck(msg.delivery_tag, false);

    _ = try ch.queueDelete("bunny-zig.test.quorum");
}

test "queue.purge does not remove unacknowledged messages" {
    const _t = h.TestTimer.start("queue.purge does not remove unacknowledged messages"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.purge-unacked";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "unacked", .{});
    try ch.publishToQueue(q, "ready-1", .{});
    try ch.publishToQueue(q, "ready-2", .{});
    _ = try ch.waitForConfirms();

    // Pull one without acking; it remains unacked, owned by this consumer.
    const got = try h.pollBasicGet(ch, q);
    try testing.expect(got != null);
    var msg = got.?;
    defer msg.deinit(h.test_allocator);

    const purged = try ch.queuePurge(q);
    // purge only removes ready messages, not the unacked one.
    try testing.expectEqual(@as(u32, 2), purged);

    // The unacked message is still owned by this channel and not in 'ready'.
    const info = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqual(@as(u32, 0), info.message_count);

    try ch.basicAck(msg.delivery_tag, false);
}

test "passive declare reports message_count and consumer_count" {
    const _t = h.TestTimer.start("passive declare reports message_count and consumer_count"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.counts";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "a", .{});
    try ch.publishToQueue(q, "b", .{});
    _ = try ch.waitForConfirms();

    const before = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqual(@as(u32, 2), before.message_count);
    try testing.expectEqual(@as(u32, 0), before.consumer_count);

    _ = try ch.basicConsume(q, "", .manual);

    var got: u32 = 0;
    var attempts: u32 = 0;
    while (got < 1 and attempts < 50) : (attempts += 1) {
        if (ch.tryRecvDelivery()) |raw| {
            var d = raw;
            defer d.deinit(h.test_allocator);
            got += 1;
        } else h.sleepMs(20);
    }

    const after = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqual(@as(u32, 1), after.consumer_count);
}
