const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// Stream queues retain history rather than removing acked messages, so a
// fresh consumer must declare an offset (x-stream-offset). basic.qos with
// global=false is required by the broker for stream consumers.

test "stream queue: x-stream-offset=first delivers history from the beginning" {
    const _t = h.TestTimer.start("stream queue: x-stream-offset=first delivers history from the beginning");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.stream-offset-first";
    _ = try ch.streamQueue(q);
    defer _ = ch.queueDelete(q) catch {};

    try ch.confirmSelect();
    for (0..3) |i| {
        var buf: [16]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "n-{d}", .{i}) catch "n";
        try ch.publishToQueue(q, body, .{});
    }
    _ = try ch.waitForConfirms();

    try ch.basicQos(10, false);

    var args_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "x-stream-offset", .value = .{ .long_string = "first" } },
    };
    const args: bunny.FieldTable = .{ .entries = &args_entries, .allocator = undefined };
    _ = try ch.basicConsumeWithTagAndArgs(q, "stream-from-first", .manual, false, args);

    var received: u32 = 0;
    for (0..80) |_| {
        if (ch.tryRecvDelivery()) |raw| {
            var d = raw;
            defer d.deinit(h.test_allocator);
            try ch.basicAck(d.delivery_tag, false);
            received += 1;
            if (received == 3) break;
        } else h.sleepMs(50);
    }
    try testing.expectEqual(@as(u32, 3), received);
}
