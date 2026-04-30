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

    const msg1 = try h.pollBasicGet(ch, "bunny-zig.test.tx-commit");
    try testing.expect(msg1 != null);
    try ch.basicAck(msg1.?.delivery_tag, false);

    const msg2 = try h.pollBasicGet(ch, "bunny-zig.test.tx-commit");
    try testing.expect(msg2 != null);
    try ch.basicAck(msg2.?.delivery_tag, false);

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
