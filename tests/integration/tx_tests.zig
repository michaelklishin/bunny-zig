const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "tx: commit publishes messages" {
    const _t = h.TestTimer.start("tx: commit publishes messages"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.tx-commit", .{ .exclusive = true, .auto_delete = true });

    try ch.txSelect();
    try ch.publishToQueue("bunny-zig.test.tx-commit", "tx message 1", .{});
    try ch.publishToQueue("bunny-zig.test.tx-commit", "tx message 2", .{});
    try ch.txCommit();

    const got_msg1 = try h.pollBasicGet(ch, "bunny-zig.test.tx-commit");
    try testing.expect(got_msg1 != null);
    var msg1 = got_msg1.?;
    defer msg1.deinit(h.test_allocator);
    try ch.basicAck(msg1.delivery_tag, false);

    const got_msg2 = try h.pollBasicGet(ch, "bunny-zig.test.tx-commit");
    try testing.expect(got_msg2 != null);
    var msg2 = got_msg2.?;
    defer msg2.deinit(h.test_allocator);
    try ch.basicAck(msg2.delivery_tag, false);

    _ = try ch.queueDelete("bunny-zig.test.tx-commit");
}

test "tx: rollback discards messages" {
    const _t = h.TestTimer.start("tx: rollback discards messages"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.tx-rollback", .{ .exclusive = true, .auto_delete = true });

    try ch.txSelect();
    try ch.publishToQueue("bunny-zig.test.tx-rollback", "will be discarded", .{});
    try ch.txRollback();

    const msg = try ch.basicGet("bunny-zig.test.tx-rollback", .manual);
    try testing.expect(msg == null);

    _ = try ch.queueDelete("bunny-zig.test.tx-rollback");
}
