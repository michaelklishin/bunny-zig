const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "basic.ack with multiple=true acknowledges every preceding delivery" {
    const _t = h.TestTimer.start("basic.ack with multiple=true acknowledges every preceding delivery");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const q = "bunny-zig.test.ack-multiple";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "one", .{});
    try ch.publishToQueue(q, "two", .{});
    try ch.publishToQueue(q, "three", .{});
    _ = try ch.waitForConfirms();

    var last_tag: u64 = 0;
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        const got = try h.pollBasicGet(ch, q);
        try testing.expect(got != null);
        var m = got.?;
        defer m.deinit(h.test_allocator);
        last_tag = m.delivery_tag;
    }

    try ch.basicAckMultiple(last_tag);

    // Closing and redeclaring confirms the queue is fully drained.
    try testing.expect((try ch.basicGet(q, .manual)) == null);
    const info = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqual(@as(u32, 0), info.message_count);
}

test "basic.nack with multiple=true and requeue redelivers every preceding message" {
    const _t = h.TestTimer.start("basic.nack with multiple=true and requeue redelivers every preceding message");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const q = "bunny-zig.test.nack-multiple";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "a", .{});
    try ch.publishToQueue(q, "b", .{});
    try ch.publishToQueue(q, "c", .{});
    _ = try ch.waitForConfirms();

    var last_tag: u64 = 0;
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        const got = try h.pollBasicGet(ch, q);
        try testing.expect(got != null);
        var m = got.?;
        defer m.deinit(h.test_allocator);
        last_tag = m.delivery_tag;
    }

    try ch.basicNack(last_tag, true, true);

    // After cumulative nack with requeue, every message comes back with redelivered=true.
    var seen: u32 = 0;
    while (seen < 3) : (seen += 1) {
        const got = try h.pollBasicGet(ch, q);
        try testing.expect(got != null);
        var m = got.?;
        defer m.deinit(h.test_allocator);
        try testing.expect(m.redelivered);
        try ch.basicAck(m.delivery_tag, false);
    }
}

test "basic.ack with an unknown delivery tag closes the channel" {
    const _t = h.TestTimer.start("basic.ack with an unknown delivery tag closes the channel"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const q = "bunny-zig.test.bad-ack";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    // basic.ack is asynchronous, so the error surfaces on the next RPC.
    try ch.basicAck(999_999, false);

    const result = ch.queueDeclare(q, .{ .passive = true });
    try testing.expectError(error.PreconditionFailed, result);
    try testing.expect(!ch.isOpen());
    // The connection itself is unaffected, so a fresh channel still works.
    try testing.expect(conn.isOpen());
}
