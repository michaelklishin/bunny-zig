const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// `confirm.select` restarts the broker's delivery-tag counter at 1, so the
// channel's pre-recovery confirm sequence state must be reset on reconnect.
// Without that reset, the post-recovery `waitForConfirms` would block forever
// because `last_confirmed_seq` lags `next_publish_seq_no`.

test "recovery: post-recovery publishes confirm under confirm.select" {
    const _t = h.TestTimer.start("recovery: post-recovery publishes confirm under confirm.select");
    defer _t.stop();

    const conn_name = "bunny-zig.test.recovery-confirm";
    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .connection_name = conn_name,
        .recovery = .{
            .enabled = true,
            .network_recovery_interval_ms = 500,
            .max_attempts = 5,
        },
    });
    defer conn.deinit();

    const ch = try conn.openChannel();
    const q = "bunny-zig.test.recovery-confirm-q";
    _ = try ch.queueDeclare(q, .{ .durable = true });
    defer _ = ch.queueDelete(q) catch {};

    try ch.confirmSelect();
    try ch.publishToQueue(q, "before-recovery", .{});
    try testing.expect(try ch.waitForConfirms());

    var http_client = try h.openHttpApiClient();
    defer http_client.deinit();
    h.forceCloseConnection(&http_client, conn_name);

    for (0..400) |_| {
        if (!conn.isOpen()) break;
        h.sleepMs(25);
    }
    for (0..400) |_| {
        if (conn.isOpen() and ch.isOpen()) break;
        h.sleepMs(25);
    }
    try testing.expect(conn.isOpen());
    try testing.expect(ch.isOpen());

    // Without the post-recovery seq reset this `waitForConfirms` would block
    // until the continuation timeout because the channel's view of seq numbers
    // would be ahead of the broker's restarted counter.
    try ch.publishToQueue(q, "after-recovery", .{});
    try testing.expect(try ch.waitForConfirms());
}

test "recovery: many confirmed publishes after reconnect line up with the broker" {
    const _t = h.TestTimer.start("recovery: many confirmed publishes after reconnect line up with the broker");
    defer _t.stop();

    const conn_name = "bunny-zig.test.recovery-confirm-many";
    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .connection_name = conn_name,
        .recovery = .{
            .enabled = true,
            .network_recovery_interval_ms = 500,
            .max_attempts = 5,
        },
    });
    defer conn.deinit();

    const ch = try conn.openChannel();
    const q = "bunny-zig.test.recovery-confirm-many-q";
    _ = try ch.queueDeclare(q, .{ .durable = true });
    defer _ = ch.queueDelete(q) catch {};

    try ch.confirmSelect();
    for (0..100) |i| {
        var buf: [16]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "pre-{d}", .{i}) catch "pre";
        try ch.publishToQueue(q, body, .{});
    }
    try testing.expect(try ch.waitForConfirms());

    var http_client = try h.openHttpApiClient();
    defer http_client.deinit();
    h.forceCloseConnection(&http_client, conn_name);

    for (0..400) |_| {
        if (!conn.isOpen()) break;
        h.sleepMs(25);
    }
    for (0..400) |_| {
        if (conn.isOpen() and ch.isOpen()) break;
        h.sleepMs(25);
    }
    try testing.expect(ch.isOpen());

    for (0..100) |i| {
        var buf: [16]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "post-{d}", .{i}) catch "post";
        try ch.publishToQueue(q, body, .{});
    }
    try testing.expect(try ch.waitForConfirms());
}
