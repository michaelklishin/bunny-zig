const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// Provoking connection.blocked requires lowering the broker's memory
// high-watermark via rabbitmqctl. The test skips when BUNNY_RABBITMQCTL is
// unset so it remains opt-in for environments that do not own the broker.

const Flags = struct {
    var blocked: std.atomic.Value(u32) = .init(0);
    var unblocked: std.atomic.Value(u32) = .init(0);

    fn onBlocked(_: []const u8) void {
        _ = blocked.fetchAdd(1, .release);
    }
    fn onUnblocked() void {
        _ = unblocked.fetchAdd(1, .release);
    }
};

test "connection.blocked and connection.unblocked notifications fire" {
    const _t = h.TestTimer.start("connection.blocked and connection.unblocked notifications fire");
    defer _t.stop();

    if (!h.runRabbitmqctl(&.{ "status" })) return error.SkipZigTest;

    Flags.blocked.store(0, .release);
    Flags.unblocked.store(0, .release);

    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    conn.on_blocked = &Flags.onBlocked;
    conn.on_unblocked = &Flags.onUnblocked;

    // Drive the watermark below current usage so the broker emits blocked.
    try testing.expect(h.runRabbitmqctl(&.{ "set_vm_memory_high_watermark", "0.0000001" }));
    defer _ = h.runRabbitmqctl(&.{ "set_vm_memory_high_watermark", "0.4" });

    // The broker only sends blocked once a publish triggers the credit check.
    const ch = try conn.openChannel();
    defer ch.close();
    try ch.publish("trigger", .{ .routing_key = "bunny-zig.test.blocked-trigger" });

    var attempts: u32 = 0;
    while (Flags.blocked.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(50);
    try testing.expect(Flags.blocked.load(.acquire) >= 1);
    try testing.expect(conn.isBlocked());

    try testing.expect(h.runRabbitmqctl(&.{ "set_vm_memory_high_watermark", "0.4" }));

    attempts = 0;
    while (Flags.unblocked.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(50);
    try testing.expect(Flags.unblocked.load(.acquire) >= 1);
    try testing.expect(!conn.isBlocked());
}
