const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

fn cleanupQueue(queue_name: []const u8) void {
    const conn = h.openTestConnection() catch return;
    defer conn.deinit();
    const ch = conn.openChannel() catch return;
    defer ch.closeChannel() catch {};
    _ = ch.queueDelete(queue_name) catch {};
}

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

    var info = try ch.temporaryQueue();
    defer info.deinit(h.test_allocator);
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

    var info = try ch.queueDeclare("", .{ .exclusive = true, .auto_delete = true });
    defer info.deinit(h.test_allocator);
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
    try testing.expectError(error.NotFound, result);
    try testing.expect(!ch.isOpen());
}

test "queueDeclarePassive convenience helper asserts existence" {
    const _t = h.TestTimer.start("queueDeclarePassive convenience helper asserts existence"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.passive-helper";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    const info = try ch.queueDeclarePassive(q);
    try testing.expectEqualSlices(u8, q, info.name);
}

test "queueDeclarePassive on a missing queue closes the channel" {
    const _t = h.TestTimer.start("queueDeclarePassive on a missing queue closes the channel"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const result = ch.queueDeclarePassive("bunny-zig.test.passive-helper-missing");
    try testing.expectError(error.NotFound, result);
    try testing.expect(!ch.isOpen());
}

// As of RabbitMQ 4.3.0, passive declares require a permission check (`configure`)
// on the target resource; 4.3.1 relaxed this to accept any permission. To keep
// the test compatible with both, we grant `configure` here. RabbitMQ's own test
// suite covers the looser permission cases.
// 4.3.0 release notes: ../main.git/release-notes/4.3.0.md
// 4.3.0 issue: https://github.com/rabbitmq/rabbitmq-server/pull/16085
// 4.3.1 relaxation PR: https://github.com/rabbitmq/rabbitmq-server/pull/16272
test "queueDeclarePassive: succeeds for a user with configure permission" {
    const _t = h.TestTimer.start("queueDeclarePassive: succeeds for a user with configure permission");
    defer _t.stop();

    if (!h.runRabbitmqctl(&.{"status"})) return error.SkipZigTest;

    const username = "bunny-zig.passive-configure";
    const password = "passive-configure-pw";
    const queue_name = "bunny-zig.test.passive-configure-q";

    {
        const setup_conn = try h.openTestConnection();
        defer setup_conn.deinit();
        const setup_ch = try setup_conn.openChannel();
        defer setup_ch.closeChannel() catch {};
        _ = try setup_ch.queueDeclare(queue_name, .{ .durable = true });
    }
    defer cleanupQueue(queue_name);

    _ = h.runRabbitmqctl(&.{ "delete_user", username });
    if (!h.runRabbitmqctl(&.{ "add_user", username, password })) return error.SkipZigTest;
    defer _ = h.runRabbitmqctl(&.{ "delete_user", username });
    // Grant configure-only on the default vhost: any permission, including
    // configure, satisfies passive declare on RabbitMQ 4.3.0+.
    if (!h.runRabbitmqctl(&.{ "set_permissions", "-p", "/", username, ".*", "^$", "^$" })) {
        return error.SkipZigTest;
    }

    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .username = username,
        .password = password,
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.queueDeclarePassive(queue_name);
    try testing.expectEqualSlices(u8, queue_name, info.name);
}

test "queueDeclarePassive: a user with no permission on the queue is refused (403)" {
    const _t = h.TestTimer.start("queueDeclarePassive: a user with no permission on the queue is refused (403)");
    defer _t.stop();

    if (!h.runRabbitmqctl(&.{"status"})) return error.SkipZigTest;

    const username = "bunny-zig.passive-nopermission";
    const password = "passive-nopermission-pw";
    const queue_name = "bunny-zig.test.passive-nopermission-q";

    {
        const setup_conn = try h.openTestConnection();
        defer setup_conn.deinit();
        const setup_ch = try setup_conn.openChannel();
        defer setup_ch.closeChannel() catch {};
        _ = try setup_ch.queueDeclare(queue_name, .{ .durable = true });
    }
    defer cleanupQueue(queue_name);

    _ = h.runRabbitmqctl(&.{ "delete_user", username });
    if (!h.runRabbitmqctl(&.{ "add_user", username, password })) return error.SkipZigTest;
    defer _ = h.runRabbitmqctl(&.{ "delete_user", username });
    // Empty patterns on every kind: the user can connect but holds no permission
    // on any resource in vhost "/".
    if (!h.runRabbitmqctl(&.{ "set_permissions", "-p", "/", username, "^$", "^$", "^$" })) {
        return error.SkipZigTest;
    }

    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .username = username,
        .password = password,
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const result = ch.queueDeclarePassive(queue_name);
    try testing.expectError(error.AccessRefused, result);
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
    try testing.expectError(error.PreconditionFailed, result);
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

    _ = try ch.basicConsume(q, .manual);

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
