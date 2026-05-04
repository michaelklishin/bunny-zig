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

    _ = try ch.basicConsume("bunny-zig.test.recovery-q", .manual);
    const got = try ch.recvDelivery();
    try testing.expect(got != null);
    var delivery = got.?;
    defer delivery.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "recovery test message", delivery.body);
    try ch.basicAck(delivery.delivery_tag, false);

    // Cleanup
    _ = try ch.queueDelete("bunny-zig.test.recovery-q");
    try ch.exchangeDelete("bunny-zig.test.recovery-ex");
}

test "recovery: server-named queue is rebound using its new name" {
    const _t = h.TestTimer.start("recovery: server-named queue is rebound using its new name");
    defer _t.stop();

    const Captured = struct {
        var old_name_buf: [128]u8 = undefined;
        var new_name_buf: [128]u8 = undefined;
        var old_len: usize = 0;
        var new_len: usize = 0;
        fn handler(event: bunny.ConnectionEvent) void {
            switch (event) {
                .recovery_queue_name_changed => |n| {
                    @memcpy(old_name_buf[0..n.old_name.len], n.old_name);
                    old_len = n.old_name.len;
                    @memcpy(new_name_buf[0..n.new_name.len], n.new_name);
                    new_len = n.new_name.len;
                },
                else => {},
            }
        }
    };
    Captured.old_len = 0;
    Captured.new_len = 0;

    const conn_name = "bunny-zig.test.recovery-server-named";
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

    try conn.event_listeners.add(h.test_allocator, &Captured.handler);

    const ch = try conn.openChannel();
    // Server-named, non-exclusive, durable (transient non-exclusive queues are
    // disallowed since RabbitMQ 4.3.0). Recovery cannot replay exclusive queues,
    // so we explicitly opt out of exclusivity here.
    var original = try ch.queueDeclare("", .{ .durable = true, .auto_delete = true });
    defer original.deinit(h.test_allocator);
    const original_name = original.name;
    try testing.expect(std.mem.startsWith(u8, original_name, "amq."));

    h.sleepMs(1200);

    var http_client = try h.openHttpApiClient();
    defer http_client.deinit();
    h.forceCloseConnection(&http_client, conn_name);

    for (0..40) |_| {
        if (!conn.isOpen()) break;
        h.sleepMs(250);
    }
    for (0..40) |_| {
        if (conn.isOpen() and Captured.new_len > 0) break;
        h.sleepMs(250);
    }
    try testing.expect(conn.isOpen());

    try testing.expect(Captured.old_len > 0);
    try testing.expect(Captured.new_len > 0);
    const old_name = Captured.old_name_buf[0..Captured.old_len];
    const new_name = Captured.new_name_buf[0..Captured.new_len];
    try testing.expectEqualSlices(u8, original_name, old_name);
    try testing.expect(!std.mem.eql(u8, old_name, new_name));
    try testing.expect(std.mem.startsWith(u8, new_name, "amq."));

    _ = ch.queueDelete(new_name) catch {};
}

test "recovery: basic.qos and consumer are replayed after reconnect" {
    const _t = h.TestTimer.start("recovery: basic.qos and consumer are replayed after reconnect");
    defer _t.stop();
    const conn_name = "bunny-zig.test.recovery-qos-consumer";
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

    const q = "bunny-zig.test.recovery-qos-q";
    _ = try ch.queueDeclare(q, .{ .durable = true });
    defer _ = ch.queueDelete(q) catch {};

    try ch.basicQos(1, false);
    _ = try ch.basicConsumeWithTag(q, "bunny-zig.recovery-consumer", .manual);

    h.sleepMs(1200);

    var http_client = try h.openHttpApiClient();
    defer http_client.deinit();
    h.forceCloseConnection(&http_client, conn_name);

    for (0..40) |_| {
        if (!conn.isOpen()) break;
        h.sleepMs(250);
    }
    for (0..40) |_| {
        if (conn.isOpen()) break;
        h.sleepMs(250);
    }
    try testing.expect(conn.isOpen());

    try ch.confirmSelect();
    try ch.publishToQueue(q, "post-recovery-1", .{});
    try ch.publishToQueue(q, "post-recovery-2", .{});
    try ch.publishToQueue(q, "post-recovery-3", .{});
    _ = try ch.waitForConfirms();

    // Prefetch=1 with manual ack: only one delivery is in flight at a time.
    var d1: ?bunny.Delivery = null;
    defer if (d1) |*x| x.deinit(h.test_allocator);
    for (0..80) |_| {
        if (ch.tryRecvDelivery()) |d| {
            d1 = d;
            break;
        }
        h.sleepMs(25);
    }
    try testing.expect(d1 != null);
    // Without an ack, no second delivery should arrive.
    h.sleepMs(150);
    try testing.expect(ch.tryRecvDelivery() == null);

    try ch.basicAck(d1.?.delivery_tag, false);
    var d2: ?bunny.Delivery = null;
    defer if (d2) |*x| x.deinit(h.test_allocator);
    for (0..80) |_| {
        if (ch.tryRecvDelivery()) |d| {
            d2 = d;
            break;
        }
        h.sleepMs(25);
    }
    try testing.expect(d2 != null);
    try ch.basicAck(d2.?.delivery_tag, false);
}

test "recovery: combined path, server-named queue plus consumer post-recovery delivers under the new name" {
    const _t = h.TestTimer.start("recovery: combined path, server-named queue plus consumer post-recovery delivers under the new name");
    defer _t.stop();

    const Captured = struct {
        var new_name_buf: [128]u8 = undefined;
        var new_len: usize = 0;
        fn handler(event: bunny.ConnectionEvent) void {
            switch (event) {
                .recovery_queue_name_changed => |n| {
                    @memcpy(new_name_buf[0..n.new_name.len], n.new_name);
                    new_len = n.new_name.len;
                },
                else => {},
            }
        }
    };
    Captured.new_len = 0;

    const conn_name = "bunny-zig.test.recovery-combined";
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
    try conn.event_listeners.add(h.test_allocator, &Captured.handler);

    const ch = try conn.openChannel();
    var original = try ch.queueDeclare("", .{ .durable = true, .auto_delete = true });
    defer original.deinit(h.test_allocator);
    try testing.expect(std.mem.startsWith(u8, original.name, "amq."));

    h.sleepMs(1200);

    var http_client = try h.openHttpApiClient();
    defer http_client.deinit();
    h.forceCloseConnection(&http_client, conn_name);

    for (0..40) |_| {
        if (!conn.isOpen()) break;
        h.sleepMs(250);
    }
    for (0..40) |_| {
        if (conn.isOpen() and Captured.new_len > 0) break;
        h.sleepMs(250);
    }
    try testing.expect(conn.isOpen());
    try testing.expect(Captured.new_len > 0);
    const new_name = Captured.new_name_buf[0..Captured.new_len];
    for (0..40) |_| {
        if (ch.isOpen()) break;
        h.sleepMs(100);
    }
    try testing.expect(ch.isOpen());

    _ = try ch.basicConsume(new_name, .manual);

    try ch.confirmSelect();
    try ch.publish("via-recovered-server-named", .{ .exchange = "", .routing_key = new_name });
    _ = try ch.waitForConfirms();

    var got: ?bunny.Delivery = null;
    defer if (got) |*x| x.deinit(h.test_allocator);
    for (0..80) |_| {
        if (ch.tryRecvDelivery()) |d| {
            got = d;
            break;
        }
        h.sleepMs(25);
    }
    try testing.expect(got != null);
    try testing.expectEqualSlices(u8, "via-recovered-server-named", got.?.body);
    try ch.basicAck(got.?.delivery_tag, false);

    _ = ch.queueDelete(new_name) catch {};
}

// Recovery exhausts after `max_attempts` and emits a `recovery_failed` event.
// Forcing the failure is awkward, point the recovery loop at an unreachable
// host so each attempt fails immediately.
test "recovery: emits recovery_failed after max_attempts is exhausted" {
    const _t = h.TestTimer.start("recovery: emits recovery_failed after max_attempts is exhausted");
    defer _t.stop();

    const Captured = struct {
        var failed_count: std.atomic.Value(u32) = .init(0);
        fn handler(event: bunny.ConnectionEvent) void {
            switch (event) {
                .recovery_failed => _ = failed_count.fetchAdd(1, .release),
                else => {},
            }
        }
    };
    Captured.failed_count.store(0, .release);

    const conn_name = "bunny-zig.test.recovery-max-attempts";
    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .connection_name = conn_name,
        .recovery = .{
            .enabled = true,
            .initial_interval_ms = 50,
            .max_interval_ms = 100,
            .max_attempts = 2,
        },
    });
    defer conn.deinit();
    try conn.event_listeners.add(h.test_allocator, &Captured.handler);

    // Swap to a port nothing is listening on so each retry fails fast.
    conn.options.port = 1;

    var http_client = try h.openHttpApiClient();
    defer http_client.deinit();
    h.forceCloseConnection(&http_client, conn_name);

    var attempts: u32 = 0;
    while (Captured.failed_count.load(.acquire) == 0 and attempts < 200) : (attempts += 1) h.sleepMs(25);
    try testing.expect(Captured.failed_count.load(.acquire) >= 1);
    try testing.expect(!conn.isOpen());
}
