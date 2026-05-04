const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "exclusive queue is locked from another connection" {
    const _t = h.TestTimer.start("exclusive queue is locked from another connection"); defer _t.stop();

    const owner = try h.openTestConnection();
    defer owner.deinit();
    const owner_ch = try owner.openChannel();
    defer owner_ch.closeChannel() catch {};

    const q = "bunny-zig.test.exclusive-cross-conn";
    _ = try owner_ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    const other = try h.openTestConnection();
    defer other.deinit();
    const other_ch = try other.openChannel();
    defer other_ch.closeChannel() catch {};

    // Touching an exclusive queue from another connection raises RESOURCE_LOCKED (405).
    const result = other_ch.basicConsume(q, .manual);
    try testing.expectError(error.ResourceLocked, result);
    try testing.expect(!other_ch.isOpen());
    // The owning connection retains exclusive access.
    try testing.expect(owner_ch.isOpen());
    try testing.expect(owner.isOpen());
}
