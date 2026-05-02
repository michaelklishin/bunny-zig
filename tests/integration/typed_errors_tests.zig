const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// Server-initiated channel closes surface as typed errors at the API call,
// and the offending method (class_id, method_id) plus reply text are
// recoverable via Channel.lastClose.

test "lastClose: returns null on a fresh channel and after a clean close" {
    const _t = h.TestTimer.start("lastClose: returns null on a fresh channel and after a clean close");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    try testing.expect(ch.lastClose() == null);
    try ch.closeChannel();
    try testing.expect(ch.lastClose() == null);
}

test "lastClose: NotFound on passive declare records reply text and offending method" {
    const _t = h.TestTimer.start("lastClose: NotFound on passive declare records reply text and offending method");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const result = ch.queueDeclare("bunny-zig.test.typed-404", .{ .passive = true });
    try testing.expectError(error.NotFound, result);

    const info = ch.lastClose() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 404), info.reply_code);
    try testing.expect(info.initiated_by_server);
    try testing.expect(info.reply_text.len > 0);
    // queue.declare lives in class 50, method 10.
    try testing.expectEqual(@as(u16, 50), info.class_id);
    try testing.expectEqual(@as(u16, 10), info.method_id);
}

test "lastClose: PreconditionFailed on inequivalent redeclare points at queue.declare" {
    const _t = h.TestTimer.start("lastClose: PreconditionFailed on inequivalent redeclare points at queue.declare");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const owner_ch = try conn.openChannel();
    defer owner_ch.closeChannel() catch {};
    const q = "bunny-zig.test.typed-406";
    _ = try owner_ch.queueDeclare(q, .{ .durable = true });
    defer _ = owner_ch.queueDelete(q) catch {};

    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};
    const result = ch.queueDeclare(q, .{ .durable = false });
    try testing.expectError(error.PreconditionFailed, result);

    const info = ch.lastClose() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 406), info.reply_code);
    try testing.expectEqual(@as(u16, 50), info.class_id);
    try testing.expectEqual(@as(u16, 10), info.method_id);
}

test "typed errors: waitForConfirms surfaces NotFound when publishing to a missing exchange" {
    const _t = h.TestTimer.start("typed errors: waitForConfirms surfaces NotFound when publishing to a missing exchange");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.confirmSelect();
    try ch.publish("orphan", .{ .exchange = "bunny-zig.test.typed-no-such-ex", .routing_key = "k" });

    const result = ch.waitForConfirms();
    try testing.expectError(error.NotFound, result);

    const info = ch.lastClose() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 404), info.reply_code);
    // basic.publish lives in class 60, method 40.
    try testing.expectEqual(@as(u16, 60), info.class_id);
    try testing.expectEqual(@as(u16, 40), info.method_id);
}

test "typed errors: ResourceLocked when consuming from a peer-exclusive queue" {
    const _t = h.TestTimer.start("typed errors: ResourceLocked when consuming from a peer-exclusive queue");
    defer _t.stop();
    const owner = try h.openTestConnection();
    defer owner.deinit();
    const owner_ch = try owner.openChannel();
    defer owner_ch.closeChannel() catch {};
    const q = "bunny-zig.test.typed-405";
    _ = try owner_ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    const peer = try h.openTestConnection();
    defer peer.deinit();
    const peer_ch = try peer.openChannel();
    defer peer_ch.closeChannel() catch {};

    const result = peer_ch.basicConsume(q, "", .manual);
    try testing.expectError(error.ResourceLocked, result);
    const info = peer_ch.lastClose() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 405), info.reply_code);
}

test "typed errors: PreconditionFailed when acking an unknown delivery tag" {
    const _t = h.TestTimer.start("typed errors: PreconditionFailed when acking an unknown delivery tag");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.typed-bad-ack";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });
    try ch.basicAck(999_999, false);

    const result = ch.queueDeclare(q, .{ .passive = true });
    try testing.expectError(error.PreconditionFailed, result);
    const info = ch.lastClose() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(u16, 406), info.reply_code);
}
