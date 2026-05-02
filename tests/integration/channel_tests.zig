const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "open and close a channel" {
    const _t = h.TestTimer.start("open and close a channel"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch = try conn.openChannel();
    try testing.expect(ch.isOpen());
    try ch.closeChannel();
    try testing.expect(!ch.isOpen());
}

test "open multiple channels" {
    const _t = h.TestTimer.start("open multiple channels"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch1 = try conn.openChannel();
    const ch2 = try conn.openChannel();
    const ch3 = try conn.openChannel();

    try testing.expect(ch1.id != ch2.id);
    try testing.expect(ch2.id != ch3.id);

    try ch1.closeChannel();
    try ch2.closeChannel();
    try ch3.closeChannel();
}

test "an error on one channel does not affect another channel" {
    const _t = h.TestTimer.start("an error on one channel does not affect another channel"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch1 = try conn.openChannel();
    defer ch1.closeChannel() catch {};
    const ch2 = try conn.openChannel();
    defer ch2.closeChannel() catch {};

    const result = ch1.queueDeclare("bunny-zig.test.no-such-isolation", .{ .passive = true });
    try testing.expectError(error.ChannelClosed, result);
    try testing.expect(!ch1.isOpen());

    // ch2 must remain fully usable.
    _ = try ch2.queueDeclare("bunny-zig.test.isolated", .{ .exclusive = true, .auto_delete = true });
}

test "RPC after explicit close returns ChannelClosed" {
    const _t = h.TestTimer.start("RPC after explicit close returns ChannelClosed"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch = try conn.openChannel();
    try ch.closeChannel();

    const result = ch.queueDeclare("bunny-zig.test.after-close", .{ .exclusive = true, .auto_delete = true });
    try testing.expectError(error.ChannelClosed, result);
}

test "channel-level error closes the channel but leaves connection open" {
    const _t = h.TestTimer.start("channel-level error closes the channel but leaves connection open");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};
    // Passive declare of a non-existent queue is a NOT_FOUND channel-level error.
    const result = ch.queueDeclare("bunny-zig.test.no-such-queue", .{ .passive = true });
    try testing.expectError(error.ChannelClosed, result);
    try testing.expect(!ch.isOpen());
    try testing.expect(conn.isOpen());

    // The connection should still be usable: open a new channel and do work on it.
    const ch2 = try conn.openChannel();
    defer ch2.closeChannel() catch {};
    _ = try ch2.queueDeclare("bunny-zig.test.after-channel-error", .{ .exclusive = true, .auto_delete = true });
}
