const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "two consumers on the same queue" {
    const _t = h.TestTimer.start("two consumers on the same queue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch1 = try conn.openChannel();
    defer ch1.closeChannel() catch {};
    const ch2 = try conn.openChannel();
    defer ch2.closeChannel() catch {};

    _ = try ch1.queueDeclare("bunny-zig.test.two-consumers", .{ .exclusive = true, .auto_delete = true });

    _ = try ch1.basicConsume("bunny-zig.test.two-consumers", "c1", .manual);
    _ = try ch2.basicConsume("bunny-zig.test.two-consumers", "c2", .manual);

    try ch1.confirmSelect();
    for (0..4) |i| {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "msg-{d}", .{i}) catch "msg";
        try ch1.publishToQueue("bunny-zig.test.two-consumers", msg, .{});
    }
    try testing.expect(try ch1.waitForConfirms());

    // Both consumers should receive messages (round-robin).
    // Poll non-blocking to avoid deadlocking if distribution is uneven.
    var count: u32 = 0;
    for (0..200) |_| {
        if (count >= 4) break;
        if (ch1.tryRecvDelivery()) |d| {
            try ch1.basicAck(d.delivery_tag, false);
            count += 1;
        }
        if (ch2.tryRecvDelivery()) |d| {
            try ch2.basicAck(d.delivery_tag, false);
            count += 1;
        }
        if (count < 4) h.sleepMs(25);
    }
    try testing.expectEqual(4, count);

    _ = try ch1.queueDelete("bunny-zig.test.two-consumers");
}

test "server-initiated basic.cancel fires when queue is deleted" {
    const _t = h.TestTimer.start("server-initiated basic.cancel fires when queue is deleted");
    defer _t.stop();

    const Cancel = struct {
        var fired: std.atomic.Value(u32) = .init(0);
        fn handler(_: []const u8) void {
            _ = fired.fetchAdd(1, .release);
        }
    };
    Cancel.fired.store(0, .release);

    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch_consume = try conn.openChannel();
    defer ch_consume.closeChannel() catch {};
    const ch_admin = try conn.openChannel();
    defer ch_admin.closeChannel() catch {};

    const q = "bunny-zig.test.server-cancel";
    // Exclusive queues are scoped to the connection, so a second channel on the
    // same connection may declare consumers and delete the queue.
    _ = try ch_consume.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });
    ch_consume.on_cancel = &Cancel.handler;
    _ = try ch_consume.basicConsume(q, "", .manual);

    _ = try ch_admin.queueDelete(q);

    var attempts: u32 = 0;
    while (Cancel.fired.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(20);
    try testing.expectEqual(1, Cancel.fired.load(.acquire));
}
