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

    const msg = try h.pollBasicGet(ch, "bunny-zig.test.quorum");
    try testing.expect(msg != null);
    try testing.expectEqualSlices(u8, "quorum message", msg.?.body);
    try ch.basicAck(msg.?.delivery_tag, false);

    _ = try ch.queueDelete("bunny-zig.test.quorum");
}
