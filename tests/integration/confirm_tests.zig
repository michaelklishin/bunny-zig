const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "publisher confirms: batch waitForConfirms" {
    const _t = h.TestTimer.start("publisher confirms: batch waitForConfirms"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.confirmSelect();
    try testing.expect(ch.confirm_mode);
    try testing.expect(!ch.confirm_tracking);

    _ = try ch.queueDeclare("bunny-zig.test.confirms-batch", .{ .exclusive = true, .auto_delete = true });

    for (0..10) |i| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "confirm message {d}", .{i}) catch "msg";
        try ch.publishToQueue("bunny-zig.test.confirms-batch", msg, .{});
    }

    const confirmed = try ch.waitForConfirms();
    try testing.expect(confirmed);

    _ = try ch.queueDelete("bunny-zig.test.confirms-batch");
}

test "publisher confirms: per-message tracking" {
    const _t = h.TestTimer.start("publisher confirms: per-message tracking"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.confirmSelectWithOptions(.{ .tracking = true });
    try testing.expect(ch.confirm_tracking);

    _ = try ch.queueDeclare("bunny-zig.test.confirms-tracking", .{ .exclusive = true, .auto_delete = true });

    // Each publish blocks until the broker confirms
    for (0..10) |i| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "tracked message {d}", .{i}) catch "msg";
        try ch.publishToQueue("bunny-zig.test.confirms-tracking", msg, .{});
    }

    // All confirms already received because tracking mode waits per message
    try testing.expectEqual(10, ch.last_confirmed_seq);

    _ = try ch.queueDelete("bunny-zig.test.confirms-tracking");
}

test "confirm.select is idempotent" {
    const _t = h.TestTimer.start("confirm.select is idempotent"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.confirmSelect();
    try ch.confirmSelect();
    try testing.expect(ch.confirm_mode);
}

test "publishing to a non-existent exchange closes the channel" {
    const _t = h.TestTimer.start("publishing to a non-existent exchange closes the channel"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.confirmSelect();
    try ch.publish("orphan", .{ .exchange = "bunny-zig.test.no-such-exchange", .routing_key = "k" });

    // The broker tears the channel down with NOT_FOUND (404) and the next
    // synchronous call surfaces it as a typed error.
    const result = ch.waitForConfirms();
    try testing.expectError(error.NotFound, result);
    try testing.expect(!ch.isOpen());
    try testing.expect(conn.isOpen());
}

test "publisher confirms: per-message tracking with backpressure" {
    const _t = h.TestTimer.start("publisher confirms: per-message tracking with backpressure"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.confirmSelectWithOptions(.{ .tracking = true, .outstanding_limit = 5 });
    try testing.expect(ch.confirm_tracking);
    try testing.expectEqual(5, ch.outstanding_limit);

    _ = try ch.queueDeclare("bunny-zig.test.confirms-backpressure", .{ .exclusive = true, .auto_delete = true });

    for (0..20) |i| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "backpressure message {d}", .{i}) catch "msg";
        try ch.publishToQueue("bunny-zig.test.confirms-backpressure", msg, .{});
    }

    try testing.expectEqual(20, ch.last_confirmed_seq);

    _ = try ch.queueDelete("bunny-zig.test.confirms-backpressure");
}
