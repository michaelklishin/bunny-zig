/// Integration tests for bunny-zig. Requires a running RabbitMQ node
/// on localhost:5672 with default credentials (guest/guest).
const std = @import("std");
const bunny = @import("bunny");

const testing = std.testing;

fn testHost() []const u8 {
    const env = std.c.getenv("BUNNY_ZIG_HOST");
    return if (env) |e| std.mem.sliceTo(e, 0) else "127.0.0.1";
}

fn testPort() u16 {
    const env = std.c.getenv("BUNNY_ZIG_PORT");
    const port_str: []const u8 = if (env) |e| std.mem.sliceTo(e, 0) else "5672";
    return std.fmt.parseInt(u16, port_str, 10) catch 5672;
}

// Use page_allocator for integration tests: the wire protocol decodes nested
// field tables that are owned by the connection. std.testing.allocator's leak
// detection flags these as leaks during connection cleanup.
const test_allocator = std.heap.page_allocator;

fn sleepMs(ms: u64) void {
    const io = std.Io.Threaded.global_single_threaded.io();
    io.sleep(.{ .nanoseconds = ms * std.time.ns_per_ms }, .boot) catch {};
}

fn openTestConnection() !*bunny.Connection {
    return bunny.Connection.open(test_allocator, .{
        .host = testHost(),
        .port = testPort(),
        .recovery = .{ .enabled = false },
    });
}

//
// Connection tests
//

test "connect with default configuration" {
    const conn = try openTestConnection();
    defer conn.deinit();
    try testing.expect(conn.is_open);
    try testing.expect(conn.negotiated_frame_max > 0);
    try testing.expect(conn.negotiated_channel_max > 0);
    try testing.expect(!conn.isBlocked());
    try testing.expect(conn.blockedReason() == null);
}

test "connect via TLS" {
    const certs_env = std.c.getenv("TLS_CERTS_DIR");
    if (certs_env == null) return error.SkipZigTest;
    const certs_dir: []const u8 = std.mem.sliceTo(certs_env.?, 0);

    var ca_path_buf: [4096]u8 = undefined;
    const ca_path = std.fmt.bufPrint(&ca_path_buf, "{s}/ca_certificate.pem", .{certs_dir}) catch return error.SkipZigTest;

    const conn = try bunny.Connection.open(test_allocator, .{
        .host = testHost(),
        .port = 5671,
        .tls = .{
            .host = testHost(),
            .ca_file = ca_path,
        },
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    try testing.expect(conn.is_open);
}

test "connect and close gracefully" {
    const conn = try openTestConnection();
    conn.close();
    try testing.expect(!conn.is_open);
    conn.deinit();
}

//
// Channel tests
//

test "open and close a channel" {
    const conn = try openTestConnection();
    defer conn.deinit();

    const ch = try conn.openChannel();
    try testing.expect(ch.is_open);
    try ch.closeChannel();
    try testing.expect(!ch.is_open);
}

test "open multiple channels" {
    const conn = try openTestConnection();
    defer conn.deinit();

    const ch1 = try conn.openChannel();
    const ch2 = try conn.openChannel();
    const ch3 = try conn.openChannel();

    try testing.expect(ch1.id != ch2.id);
    try testing.expect(ch2.id != ch3.id);

    try ch1.closeChannel();
    try ch2.closeChannel();
    try ch3.closeChannel();
}

//
// Queue tests
//

test "declare and delete a queue" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.queueDeclare("bunny-zig.test.declare-delete", .{ .auto_delete = true });
    try testing.expectEqualSlices(u8, "bunny-zig.test.declare-delete", info.name);

    _ = try ch.queueDelete("bunny-zig.test.declare-delete");
}

test "declare a durable queue" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.durableQueue("bunny-zig.test.durable");
    try testing.expectEqualSlices(u8, "bunny-zig.test.durable", info.name);

    _ = try ch.queueDelete("bunny-zig.test.durable");
}

test "declare a temporary queue" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.temporaryQueue();
    try testing.expect(info.name.len > 0);
}

test "queue purge" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.purge", .{ .auto_delete = true });
    try ch.confirmSelect();

    for (0..5) |_| {
        try ch.publishToQueue("bunny-zig.test.purge", "test message", .{});
    }

    const confirmed = try ch.waitForConfirms();
    try testing.expect(confirmed);

    const purged = try ch.queuePurge("bunny-zig.test.purge");
    try testing.expect(purged >= 5);
    _ = try ch.queueDelete("bunny-zig.test.purge");
}

//
// Exchange tests
//

test "declare and delete a fanout exchange" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareFanout("bunny-zig.test.fanout");
    try ch.exchangeDelete("bunny-zig.test.fanout");
}

test "declare and delete a topic exchange" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareTopic("bunny-zig.test.topic");
    try ch.exchangeDelete("bunny-zig.test.topic");
}

test "declare and delete a direct exchange" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareDirect("bunny-zig.test.direct");
    try ch.exchangeDelete("bunny-zig.test.direct");
}

test "declare and delete a headers exchange" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareHeaders("bunny-zig.test.headers");
    try ch.exchangeDelete("bunny-zig.test.headers");
}

//
// Binding tests
//

test "bind and unbind a queue" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareDirect("bunny-zig.test.bind-exchange");
    _ = try ch.queueDeclare("bunny-zig.test.bind-queue", .{ .auto_delete = true });

    try ch.queueBind("bunny-zig.test.bind-queue", "bunny-zig.test.bind-exchange", "test.key");
    try ch.queueUnbind("bunny-zig.test.bind-queue", "bunny-zig.test.bind-exchange", "test.key");

    _ = try ch.queueDelete("bunny-zig.test.bind-queue");
    try ch.exchangeDelete("bunny-zig.test.bind-exchange");
}

test "exchange-to-exchange binding" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareFanout("bunny-zig.test.e2e-source");
    try ch.declareFanout("bunny-zig.test.e2e-dest");

    try ch.exchangeBind("bunny-zig.test.e2e-dest", "bunny-zig.test.e2e-source", "");
    try ch.exchangeUnbind("bunny-zig.test.e2e-dest", "bunny-zig.test.e2e-source", "");

    try ch.exchangeDelete("bunny-zig.test.e2e-source");
    try ch.exchangeDelete("bunny-zig.test.e2e-dest");
}

//
// Publish and consume tests
//

test "publish and basic.get" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.basic-get", .{ .auto_delete = true });
    try ch.confirmSelect();

    try ch.publishToQueue("bunny-zig.test.basic-get", "Hello from bunny-zig!", BasicProperties.persistent);
    try testing.expect(try ch.waitForConfirms());

    const result = try ch.basicGet("bunny-zig.test.basic-get", .manual);
    try testing.expect(result != null);
    const msg = result.?;
    try testing.expectEqualSlices(u8, "Hello from bunny-zig!", msg.body);

    try ch.basicAck(msg.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.basic-get");
}

test "publish and consume with manual ack" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.consume", .{ .auto_delete = true });
    _ = try ch.basicConsume("bunny-zig.test.consume", "test-consumer", .manual);

    try ch.publishToQueue("bunny-zig.test.consume", "consumed message", .{});

    const delivery = try ch.recvDelivery();
    try testing.expect(delivery != null);
    const msg = delivery.?;
    try testing.expectEqualSlices(u8, "consumed message", msg.body);

    try ch.basicAck(msg.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.consume");
}

test "publish with properties" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.props", .{ .auto_delete = true });
    try ch.confirmSelect();

    const props = BasicProperties.default
        .withContentType("application/json")
        .asPersistent()
        .withMessageId("msg-001")
        .withCorrelationId("corr-001")
        .withAppId("bunny-zig-test");

    try ch.publish("{\"key\": \"value\"}", .{
        .routing_key = "bunny-zig.test.props",
        .properties = props,
    });
    try testing.expect(try ch.waitForConfirms());

    const result = try ch.basicGet("bunny-zig.test.props", .manual);
    try testing.expect(result != null);
    const msg = result.?;
    try testing.expectEqualSlices(u8, "application/json", msg.properties.content_type.?);
    try testing.expectEqual(@as(u8, 2), msg.properties.delivery_mode.?);
    try testing.expectEqualSlices(u8, "msg-001", msg.properties.message_id.?);

    try ch.basicAck(msg.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.props");
}

//
// QoS tests
//

test "basic qos" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.basicQos(10, false);
}

//
// Publisher confirm tests
//

test "publisher confirms: batch waitForConfirms" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.confirmSelect();
    try testing.expect(ch.confirm_mode);
    try testing.expect(!ch.confirm_tracking);

    _ = try ch.queueDeclare("bunny-zig.test.confirms-batch", .{ .auto_delete = true });

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
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.confirmSelectWithOptions(.{ .tracking = true });
    try testing.expect(ch.confirm_tracking);

    _ = try ch.queueDeclare("bunny-zig.test.confirms-tracking", .{ .auto_delete = true });

    // Each publish blocks until the broker confirms
    for (0..10) |i| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "tracked message {d}", .{i}) catch "msg";
        try ch.publishToQueue("bunny-zig.test.confirms-tracking", msg, .{});
    }

    // All confirms already received because tracking mode waits per message
    try testing.expectEqual(@as(u64, 10), ch.last_confirmed_seq);

    _ = try ch.queueDelete("bunny-zig.test.confirms-tracking");
}

test "publisher confirms: per-message tracking with backpressure" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.confirmSelectWithOptions(.{ .tracking = true, .outstanding_limit = 5 });
    try testing.expect(ch.confirm_tracking);
    try testing.expectEqual(@as(u32, 5), ch.outstanding_limit);

    _ = try ch.queueDeclare("bunny-zig.test.confirms-backpressure", .{ .auto_delete = true });

    for (0..20) |i| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "backpressure message {d}", .{i}) catch "msg";
        try ch.publishToQueue("bunny-zig.test.confirms-backpressure", msg, .{});
    }

    try testing.expectEqual(@as(u64, 20), ch.last_confirmed_seq);

    _ = try ch.queueDelete("bunny-zig.test.confirms-backpressure");
}

//
// Reject and nack tests
//

test "reject and requeue" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.reject", .{ .auto_delete = true });
    try ch.confirmSelect();

    // Use a consumer for reliable delivery
    _ = try ch.basicConsume("bunny-zig.test.reject", "", .manual);

    try ch.publishToQueue("bunny-zig.test.reject", "rejected message", .{});
    try testing.expect(try ch.waitForConfirms());

    const delivery1 = try ch.recvDelivery();
    try testing.expect(delivery1 != null);
    try ch.basicReject(delivery1.?.delivery_tag, true);

    // Wait for redelivered message via consumer
    const delivery2 = try ch.recvDelivery();
    try testing.expect(delivery2 != null);
    try testing.expect(delivery2.?.redelivered);
    try ch.basicAck(delivery2.?.delivery_tag, false);

    _ = try ch.queueDelete("bunny-zig.test.reject");
}

test "nack with requeue" {
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.nack", .{ .auto_delete = true });
    try ch.confirmSelect();

    _ = try ch.basicConsume("bunny-zig.test.nack", "", .manual);

    try ch.publishToQueue("bunny-zig.test.nack", "nacked message", .{});
    try testing.expect(try ch.waitForConfirms());

    const delivery1 = try ch.recvDelivery();
    try testing.expect(delivery1 != null);
    try ch.basicNack(delivery1.?.delivery_tag, false, true);

    const delivery2 = try ch.recvDelivery();
    try testing.expect(delivery2 != null);
    try ch.basicAck(delivery2.?.delivery_tag, false);

    _ = try ch.queueDelete("bunny-zig.test.nack");
}

/// Poll basic.get until a message arrives or attempts are exhausted.
fn pollBasicGet(ch: *bunny.Channel, queue: []const u8) !?bunny.GetResult {
    for (0..100) |_| {
        if (try ch.basicGet(queue, .manual)) |result| {
            return result;
        }
        sleepMs(50);
    }
    return null;
}

const BasicProperties = bunny.BasicProperties;

//
// Recovery tests (require HTTP API client)
//

const api = @import("rabbitmq_http_api_client");

fn openHttpApiClient() !api.Client {
    const io = std.Io.Threaded.global_single_threaded.io();
    return api.Client.init(std.heap.c_allocator, io, .{});
}

/// Force-close connections via the HTTP API. Closes all connections for "guest",
/// then waits for the server to process the closure.
fn forceCloseConnections(http_client: *api.Client) void {
    sleepMs(1200);
    http_client.closeUserConnections("guest", "closed by bunny-zig tests", true) catch {};
    sleepMs(500);
}

test "recovery: reconnects after forced close" {
    const conn_name = "bunny-zig.test.recovery";
    const conn = try bunny.Connection.open(test_allocator, .{
        .host = testHost(),
        .port = testPort(),
        .connection_name = conn_name,
        .recovery = .{
            .enabled = true,
            .initial_interval_ms = 500,
            .max_interval_ms = 2_000,
            .max_attempts = 5,
        },
    });
    defer conn.deinit();

    try testing.expect(conn.is_open);

    // Allow stats to be emitted
    sleepMs(1200);

    var http_client = try openHttpApiClient();
    defer http_client.deinit();

    forceCloseConnections(&http_client);

    // Wait for the client to detect the closure and recover
    for (0..40) |_| {
        if (!conn.is_open) break;
        sleepMs(250);
    }
    for (0..40) |_| {
        if (conn.is_open) break;
        sleepMs(250);
    }
    try testing.expect(conn.is_open);
}

test "recovery: topology is replayed after reconnect" {
    const conn_name = "bunny-zig.test.recovery-topology";
    const conn = try bunny.Connection.open(test_allocator, .{
        .host = testHost(),
        .port = testPort(),
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

    sleepMs(1200);

    var http_client = try openHttpApiClient();
    defer http_client.deinit();

    forceCloseConnections(&http_client);

    // Wait for closure and recovery
    for (0..40) |_| {
        if (!conn.is_open) break;
        sleepMs(250);
    }
    for (0..40) |_| {
        if (conn.is_open) break;
        sleepMs(250);
    }
    try testing.expect(conn.is_open);

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
