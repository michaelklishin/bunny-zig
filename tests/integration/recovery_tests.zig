const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "recovery: reconnects after forced close" {
    const _t = h.TestTimer.start("recovery: reconnects after forced close"); defer _t.stop();
    const conn_name = "bunny-zig.test.recovery";
    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .connection_name = conn_name,
        .recovery = .{
            .enabled = true,
            .initial_interval_ms = 500,
            .max_interval_ms = 2_000,
            .max_attempts = 5,
        },
    });
    defer conn.deinit();

    try testing.expect(conn.isOpen());

    // Allow stats to be emitted
    h.sleepMs(1200);

    var http_client = try h.openHttpApiClient();
    defer http_client.deinit();

    h.forceCloseConnection(&http_client, conn_name);

    // Wait for the client to detect the closure and recover
    for (0..40) |_| {
        if (!conn.isOpen()) break;
        h.sleepMs(250);
    }
    for (0..40) |_| {
        if (conn.isOpen()) break;
        h.sleepMs(250);
    }
    try testing.expect(conn.isOpen());
}

test "recovery: topology is replayed after reconnect" {
    const _t = h.TestTimer.start("recovery: topology is replayed after reconnect"); defer _t.stop();
    const conn_name = "bunny-zig.test.recovery-topology";
    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .connection_name = conn_name,
        .recovery = .{
            .enabled = true,
            .initial_interval_ms = 500,
            .max_interval_ms = 2_000,
            .max_attempts = 5,
        },
    });
    defer conn.deinit();

    const ch = try conn.openChannel();

    // Declare topology
    _ = try ch.queueDeclare("bunny-zig.test.recovery-q", .{ .durable = true });
    try ch.declareDirect("bunny-zig.test.recovery-ex");
    try ch.queueBind("bunny-zig.test.recovery-q", "bunny-zig.test.recovery-ex", "test.key");

    h.sleepMs(1200);

    var http_client = try h.openHttpApiClient();
    defer http_client.deinit();

    h.forceCloseConnection(&http_client, conn_name);

    // Wait for closure and recovery
    for (0..40) |_| {
        if (!conn.isOpen()) break;
        h.sleepMs(250);
    }
    for (0..40) |_| {
        if (conn.isOpen()) break;
        h.sleepMs(250);
    }
    try testing.expect(conn.isOpen());

    // Verify topology was replayed: publish to the exchange, consume from the queue
    try ch.confirmSelect();
    try ch.publish("recovery test message", .{
        .exchange = "bunny-zig.test.recovery-ex",
        .routing_key = "test.key",
    });
    _ = try ch.waitForConfirms();

    _ = try ch.basicConsume("bunny-zig.test.recovery-q", "", .manual);
    const delivery = try ch.recvDelivery();
    try testing.expect(delivery != null);
    try testing.expectEqualSlices(u8, "recovery test message", delivery.?.body);
    try ch.basicAck(delivery.?.delivery_tag, false);

    // Cleanup
    _ = try ch.queueDelete("bunny-zig.test.recovery-q");
    try ch.exchangeDelete("bunny-zig.test.recovery-ex");
}
