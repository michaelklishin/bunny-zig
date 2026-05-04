const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// Each AMQP 0-9-1 reply code surfaces as a distinct typed error from the API
// call, and the channel exposes the reply text and offending method via lastClose.

test "channel error taxonomy: NOT_FOUND (404) on passive declare of a missing queue" {
    const _t = h.TestTimer.start("channel error taxonomy: NOT_FOUND (404) on passive declare of a missing queue");
    defer _t.stop();

    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const result = ch.queueDeclare("bunny-zig.test.taxonomy.404", .{ .passive = true });
    try testing.expectError(error.NotFound, result);

    const info = ch.lastClose() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 404), info.reply_code);
    try testing.expect(info.initiated_by_server);
    try testing.expect(info.reply_text.len > 0);
}

test "channel error taxonomy: PRECONDITION_FAILED (406) on inequivalent redeclare" {
    const _t = h.TestTimer.start("channel error taxonomy: PRECONDITION_FAILED (406) on inequivalent redeclare");
    defer _t.stop();

    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch_owner = try conn.openChannel();
    defer ch_owner.closeChannel() catch {};
    const q = "bunny-zig.test.taxonomy.406";
    _ = try ch_owner.queueDeclare(q, .{ .durable = true });
    defer _ = ch_owner.queueDelete(q) catch {};

    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const result = ch.queueDeclare(q, .{ .durable = false });
    try testing.expectError(error.PreconditionFailed, result);

    const info = ch.lastClose() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 406), info.reply_code);
}

test "channel error taxonomy: RESOURCE_LOCKED (405) on cross-connection exclusive consume" {
    const _t = h.TestTimer.start("channel error taxonomy: RESOURCE_LOCKED (405) on cross-connection exclusive consume");
    defer _t.stop();

    const owner = try h.openTestConnection();
    defer owner.deinit();
    const owner_ch = try owner.openChannel();
    defer owner_ch.closeChannel() catch {};
    const q = "bunny-zig.test.taxonomy.405";
    _ = try owner_ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    const other = try h.openTestConnection();
    defer other.deinit();
    const other_ch = try other.openChannel();
    defer other_ch.closeChannel() catch {};

    const result = other_ch.basicConsume(q, .manual);
    try testing.expectError(error.ResourceLocked, result);

    const info = other_ch.lastClose() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 405), info.reply_code);
}
