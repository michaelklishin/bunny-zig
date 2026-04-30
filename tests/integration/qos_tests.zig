const std = @import("std");
const h = @import("test_helpers.zig");

test "basic qos" {
    const _t = h.TestTimer.start("basic qos"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.basicQos(10, false);
}
