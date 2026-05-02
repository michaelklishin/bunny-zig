const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// Channel-level errors all surface as error.ChannelClosed at the API. The
// AMQP reply code is delivered on the channel close event, so each test
// records it via an event listener and asserts the exact code.

// `Captured` uses static globals: relies on Zig's serial test execution.
const Captured = struct {
    var code: std.atomic.Value(u16) = .init(0);
    fn handler(ev: bunny.ChannelEvent) void {
        switch (ev) {
            .closed => |c| _ = code.store(c.code, .release),
            else => {},
        }
    }
};

fn waitForCode() u16 {
    var attempts: u32 = 0;
    while (Captured.code.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(10);
    return Captured.code.load(.acquire);
}

test "channel error taxonomy: NOT_FOUND (404) on passive declare of a missing queue" {
    const _t = h.TestTimer.start("channel error taxonomy: NOT_FOUND (404) on passive declare of a missing queue");
    defer _t.stop();
    Captured.code.store(0, .release);

    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};
    try ch.event_listeners.add(h.test_allocator, &Captured.handler);

    const result = ch.queueDeclare("bunny-zig.test.taxonomy.404", .{ .passive = true });
    try testing.expectError(error.ChannelClosed, result);
    try testing.expectEqual(404, waitForCode());
}

test "channel error taxonomy: PRECONDITION_FAILED (406) on inequivalent redeclare" {
    const _t = h.TestTimer.start("channel error taxonomy: PRECONDITION_FAILED (406) on inequivalent redeclare");
    defer _t.stop();
    Captured.code.store(0, .release);

    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch_owner = try conn.openChannel();
    defer ch_owner.closeChannel() catch {};
    const q = "bunny-zig.test.taxonomy.406";
    _ = try ch_owner.queueDeclare(q, .{ .durable = true });
    defer _ = ch_owner.queueDelete(q) catch {};

    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};
    try ch.event_listeners.add(h.test_allocator, &Captured.handler);

    const result = ch.queueDeclare(q, .{ .durable = false });
    try testing.expectError(error.ChannelClosed, result);
    try testing.expectEqual(406, waitForCode());
}

test "channel error taxonomy: RESOURCE_LOCKED (405) on cross-connection exclusive consume" {
    const _t = h.TestTimer.start("channel error taxonomy: RESOURCE_LOCKED (405) on cross-connection exclusive consume");
    defer _t.stop();
    Captured.code.store(0, .release);

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
    try other_ch.event_listeners.add(h.test_allocator, &Captured.handler);

    const result = other_ch.basicConsume(q, "", .manual);
    try testing.expectError(error.ChannelClosed, result);
    try testing.expectEqual(405, waitForCode());
}
