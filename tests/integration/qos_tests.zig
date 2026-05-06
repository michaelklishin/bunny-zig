const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "basic qos" {
    const _t = h.TestTimer.start("basic qos"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    try ch.basicQos(10, false);
}

test "basic.qos with global=false caps deliveries per consumer" {
    const _t = h.TestTimer.start("basic.qos with global=false caps deliveries per consumer"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const q = "bunny-zig.test.qos-per-consumer";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    for (0..5) |i| {
        var buf: [16]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "m-{d}", .{i}) catch "m";
        try ch.publishToQueue(q, body, .{});
    }
    _ = try ch.waitForConfirms();

    try ch.basicQos(2, false);
    _ = try ch.basicConsume(q, .manual);

    var got: u32 = 0;
    // 200 ms negative-assertion budget: confirm the 3rd delivery never arrives.
    for (0..8) |_| {
        if (ch.tryRecvDelivery()) |raw| {
            var d = raw;
            defer d.deinit(h.test_allocator);
            got += 1;
        } else h.sleepMs(25);
        if (got >= 3) break;
    }
    // With prefetch=2 and no acks, only 2 may be in flight at once.
    try testing.expectEqual(@as(u32, 2), got);
}

test "basic.qos(0) means unlimited delivery" {
    const _t = h.TestTimer.start("basic.qos(0) means unlimited delivery"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const q = "bunny-zig.test.qos-zero";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    for (0..5) |i| {
        var buf: [16]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "m-{d}", .{i}) catch "m";
        try ch.publishToQueue(q, body, .{});
    }
    _ = try ch.waitForConfirms();

    try ch.basicQos(0, false);
    _ = try ch.basicConsume(q, .manual);

    var got: u32 = 0;
    for (0..80) |_| {
        if (ch.tryRecvDelivery()) |raw| {
            var d = raw;
            defer d.deinit(h.test_allocator);
            try ch.basicAck(d.delivery_tag, false);
            got += 1;
            if (got == 5) break;
        } else h.sleepMs(20);
    }
    try testing.expectEqual(@as(u32, 5), got);
}
