const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// Per-consumer prefetch (basic.qos with global=false) is captured at consume
// time and replayed before each consumer is re-attached on recovery.
test "recovery: per-consumer prefetch is replayed after reconnect" {
    const _t = h.TestTimer.start("recovery: per-consumer prefetch is replayed after reconnect");
    defer _t.stop();

    const conn_name = "bunny-zig.test.recovery-qos-per-consumer";
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
    const q = "bunny-zig.test.recovery-qos-per-consumer-q";
    _ = try ch.queueDeclare(q, .{ .durable = true });
    defer _ = ch.queueDelete(q) catch {};

    try ch.basicQos(2, false);
    _ = try ch.basicConsumeWithTag(q, "bunny-zig.recovery-qos-tag", .manual);

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

    try ch.confirmSelect();
    try ch.publishToQueue(q, "m-1", .{});
    try ch.publishToQueue(q, "m-2", .{});
    try ch.publishToQueue(q, "m-3", .{});
    try ch.publishToQueue(q, "m-4", .{});
    try testing.expect(try ch.waitForConfirms());

    // With prefetch=2 and no acks, exactly two deliveries may be in flight.
    var got: u32 = 0;
    var deliveries: [4]bunny.Delivery = undefined;
    defer for (deliveries[0..got]) |*d| d.deinit(h.test_allocator);

    // 500 ms negative-assertion budget: confirm the 3rd delivery never arrives.
    for (0..20) |_| {
        if (ch.tryRecvDelivery()) |d| {
            deliveries[got] = d;
            got += 1;
            if (got >= 3) break;
        } else h.sleepMs(25);
    }
    try testing.expectEqual(@as(u32, 2), got);
}
