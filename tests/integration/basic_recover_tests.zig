const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "basic.recover with requeue=true redelivers unacknowledged messages" {
    const _t = h.TestTimer.start("basic.recover with requeue=true redelivers unacknowledged messages");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.recover-requeue";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "one", .{});
    try ch.publishToQueue(q, "two", .{});
    _ = try ch.waitForConfirms();

    _ = try ch.basicConsume(q, "", .manual);

    var first: u32 = 0;
    while (first < 2) : (first += 1) {
        const got = try ch.recvDelivery();
        try testing.expect(got != null);
        var m = got.?;
        defer m.deinit(h.test_allocator);
        try testing.expect(!m.redelivered);
    }

    try ch.basicRecover(true);

    var redelivered: u32 = 0;
    for (0..40) |_| {
        if (ch.tryRecvDelivery()) |raw| {
            var d = raw;
            defer d.deinit(h.test_allocator);
            try testing.expect(d.redelivered);
            try ch.basicAck(d.delivery_tag, false);
            redelivered += 1;
            if (redelivered == 2) break;
        } else h.sleepMs(25);
    }
    try testing.expectEqual(@as(u32, 2), redelivered);
}

test "basic.recover with auto-ack does nothing because nothing is unacked" {
    const _t = h.TestTimer.start("basic.recover with auto-ack does nothing because nothing is unacked");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.recover-autoack";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "auto", .{});
    _ = try ch.waitForConfirms();

    _ = try ch.basicConsume(q, "", .automatic);
    const got = try ch.recvDelivery();
    try testing.expect(got != null);
    var m = got.?;
    defer m.deinit(h.test_allocator);

    try ch.basicRecover(true);
    h.sleepMs(150);
    try testing.expect(ch.tryRecvDelivery() == null);
}
