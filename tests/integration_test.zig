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

const TestTimer = struct {
    name: []const u8,
    start_ns: i64,

    fn start(name: []const u8) TestTimer {
        const io = std.Io.Threaded.global_single_threaded.io();
        return .{ .name = name, .start_ns = @intCast(std.Io.Clock.awake.now(io).nanoseconds) };
    }

    fn stop(self: TestTimer) void {
        const io = std.Io.Threaded.global_single_threaded.io();
        const now_ns: i64 = @intCast(std.Io.Clock.awake.now(io).nanoseconds);
        const elapsed_ms = @as(u64, @intCast(@divTrunc(now_ns - self.start_ns, std.time.ns_per_ms)));
        std.debug.print("[test_timing] {s}: {d}ms\n", .{ self.name, elapsed_ms });
    }
};

//
// Connection tests
//

test "connect with default configuration" {
    const _t = TestTimer.start("connect with default configuration"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    try testing.expect(conn.isOpen());
    try testing.expect(conn.negotiated_frame_max > 0);
    try testing.expect(conn.negotiated_channel_max > 0);
    try testing.expect(!conn.isBlocked());
    try testing.expect(conn.blockedReason() == null);
}

test "connect via TLS" {
    const _t = TestTimer.start("connect via TLS"); defer _t.stop();
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
    try testing.expect(conn.isOpen());
}

test "connect and close gracefully" {
    const _t = TestTimer.start("connect and close gracefully"); defer _t.stop();
    const conn = try openTestConnection();
    conn.close();
    try testing.expect(!conn.isOpen());
    conn.deinit();
}

//
// Channel tests
//

test "open and close a channel" {
    const _t = TestTimer.start("open and close a channel"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();

    const ch = try conn.openChannel();
    try testing.expect(ch.isOpen());
    try ch.closeChannel();
    try testing.expect(!ch.isOpen());
}

test "open multiple channels" {
    const _t = TestTimer.start("open multiple channels"); defer _t.stop();
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
    const _t = TestTimer.start("declare and delete a queue"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.queueDeclare("bunny-zig.test.declare-delete", .{ .exclusive = true, .auto_delete = true });
    try testing.expectEqualSlices(u8, "bunny-zig.test.declare-delete", info.name);

    _ = try ch.queueDelete("bunny-zig.test.declare-delete");
}

test "declare a durable queue" {
    const _t = TestTimer.start("declare a durable queue"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.durableQueue("bunny-zig.test.durable");
    try testing.expectEqualSlices(u8, "bunny-zig.test.durable", info.name);

    _ = try ch.queueDelete("bunny-zig.test.durable");
}

test "declare a temporary queue" {
    const _t = TestTimer.start("declare a temporary queue"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const info = try ch.temporaryQueue();
    try testing.expect(info.name.len > 0);
}

test "queue purge" {
    const _t = TestTimer.start("queue purge"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.purge", .{ .exclusive = true, .auto_delete = true });
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
    const _t = TestTimer.start("declare and delete a fanout exchange"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareFanout("bunny-zig.test.fanout");
    try ch.exchangeDelete("bunny-zig.test.fanout");
}

test "declare and delete a topic exchange" {
    const _t = TestTimer.start("declare and delete a topic exchange"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareTopic("bunny-zig.test.topic");
    try ch.exchangeDelete("bunny-zig.test.topic");
}

test "declare and delete a direct exchange" {
    const _t = TestTimer.start("declare and delete a direct exchange"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareDirect("bunny-zig.test.direct");
    try ch.exchangeDelete("bunny-zig.test.direct");
}

test "declare and delete a headers exchange" {
    const _t = TestTimer.start("declare and delete a headers exchange"); defer _t.stop();
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
    const _t = TestTimer.start("bind and unbind a queue"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareDirect("bunny-zig.test.bind-exchange");
    _ = try ch.queueDeclare("bunny-zig.test.bind-queue", .{ .exclusive = true, .auto_delete = true });

    try ch.queueBind("bunny-zig.test.bind-queue", "bunny-zig.test.bind-exchange", "test.key");
    try ch.queueUnbind("bunny-zig.test.bind-queue", "bunny-zig.test.bind-exchange", "test.key");

    _ = try ch.queueDelete("bunny-zig.test.bind-queue");
    try ch.exchangeDelete("bunny-zig.test.bind-exchange");
}

test "exchange-to-exchange binding" {
    const _t = TestTimer.start("exchange-to-exchange binding"); defer _t.stop();
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
    const _t = TestTimer.start("publish and basic.get"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.basic-get", .{ .exclusive = true, .auto_delete = true });
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
    const _t = TestTimer.start("publish and consume with manual ack"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.consume", .{ .exclusive = true, .auto_delete = true });
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
    const _t = TestTimer.start("publish with properties"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.props", .{ .exclusive = true, .auto_delete = true });
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
    try testing.expectEqual(2, msg.properties.delivery_mode.?);
    try testing.expectEqualSlices(u8, "msg-001", msg.properties.message_id.?);

    try ch.basicAck(msg.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.props");
}

//
// QoS tests
//

test "basic qos" {
    const _t = TestTimer.start("basic qos"); defer _t.stop();
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
    const _t = TestTimer.start("publisher confirms: batch waitForConfirms"); defer _t.stop();
    const conn = try openTestConnection();
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
    const _t = TestTimer.start("publisher confirms: per-message tracking"); defer _t.stop();
    const conn = try openTestConnection();
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

test "publisher confirms: per-message tracking with backpressure" {
    const _t = TestTimer.start("publisher confirms: per-message tracking with backpressure"); defer _t.stop();
    const conn = try openTestConnection();
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

//
// Reject and nack tests
//

test "reject and requeue" {
    const _t = TestTimer.start("reject and requeue"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.reject", .{ .exclusive = true, .auto_delete = true });
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
    const _t = TestTimer.start("nack with requeue"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.nack", .{ .exclusive = true, .auto_delete = true });
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

/// Force-close a specific connection via the HTTP API,
/// identified by its client-provided connection name.
fn forceCloseConnection(http_client: *api.Client, connection_name: []const u8) void {
    sleepMs(1200);
    const conns = (http_client.listConnections() catch return).value;
    for (conns) |ci| {
        const cp = ci.client_properties orelse continue;
        const cn = cp.connection_name orelse continue;
        if (std.mem.eql(u8, cn, connection_name)) {
            http_client.closeConnection(ci.name, "closed by bunny-zig tests", true) catch {};
        }
    }
    sleepMs(500);
}

test "recovery: reconnects after forced close" {
    const _t = TestTimer.start("recovery: reconnects after forced close"); defer _t.stop();
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

    try testing.expect(conn.isOpen());

    // Allow stats to be emitted
    sleepMs(1200);

    var http_client = try openHttpApiClient();
    defer http_client.deinit();

    forceCloseConnection(&http_client, conn_name);

    // Wait for the client to detect the closure and recover
    for (0..40) |_| {
        if (!conn.isOpen()) break;
        sleepMs(250);
    }
    for (0..40) |_| {
        if (conn.isOpen()) break;
        sleepMs(250);
    }
    try testing.expect(conn.isOpen());
}

test "recovery: topology is replayed after reconnect" {
    const _t = TestTimer.start("recovery: topology is replayed after reconnect"); defer _t.stop();
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

    forceCloseConnection(&http_client, conn_name);

    // Wait for closure and recovery
    for (0..40) |_| {
        if (!conn.isOpen()) break;
        sleepMs(250);
    }
    for (0..40) |_| {
        if (conn.isOpen()) break;
        sleepMs(250);
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

//
// Transaction tests
//

test "tx: commit publishes messages" {
    const _t = TestTimer.start("tx: commit publishes messages"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.tx-commit", .{ .exclusive = true, .auto_delete = true });

    try ch.txSelect();
    try ch.publishToQueue("bunny-zig.test.tx-commit", "tx message 1", .{});
    try ch.publishToQueue("bunny-zig.test.tx-commit", "tx message 2", .{});
    try ch.txCommit();

    const msg1 = try pollBasicGet(ch, "bunny-zig.test.tx-commit");
    try testing.expect(msg1 != null);
    try ch.basicAck(msg1.?.delivery_tag, false);

    const msg2 = try pollBasicGet(ch, "bunny-zig.test.tx-commit");
    try testing.expect(msg2 != null);
    try ch.basicAck(msg2.?.delivery_tag, false);

    _ = try ch.queueDelete("bunny-zig.test.tx-commit");
}

test "tx: rollback discards messages" {
    const _t = TestTimer.start("tx: rollback discards messages"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.tx-rollback", .{ .exclusive = true, .auto_delete = true });

    try ch.txSelect();
    try ch.publishToQueue("bunny-zig.test.tx-rollback", "will be discarded", .{});
    try ch.txRollback();

    const msg = try ch.basicGet("bunny-zig.test.tx-rollback", .manual);
    try testing.expect(msg == null);

    _ = try ch.queueDelete("bunny-zig.test.tx-rollback");
}

//
// Quorum queue tests
//

test "declare and use a quorum queue" {
    const _t = TestTimer.start("declare and use a quorum queue"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const qi = try ch.quorumQueue("bunny-zig.test.quorum");
    try testing.expect(qi.message_count == 0);

    try ch.confirmSelect();
    try ch.publishToQueue("bunny-zig.test.quorum", "quorum message", .{});
    try testing.expect(try ch.waitForConfirms());

    const msg = try pollBasicGet(ch, "bunny-zig.test.quorum");
    try testing.expect(msg != null);
    try testing.expectEqualSlices(u8, "quorum message", msg.?.body);
    try ch.basicAck(msg.?.delivery_tag, false);

    _ = try ch.queueDelete("bunny-zig.test.quorum");
}

//
// Edge case tests
//

test "publish and consume empty body" {
    const _t = TestTimer.start("publish and consume empty body"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.empty-body", .{ .exclusive = true, .auto_delete = true });
    try ch.confirmSelect();

    try ch.publishToQueue("bunny-zig.test.empty-body", "", .{});
    try testing.expect(try ch.waitForConfirms());

    const msg = try pollBasicGet(ch, "bunny-zig.test.empty-body");
    try testing.expect(msg != null);
    try testing.expectEqual(0, msg.?.body.len);

    try ch.basicAck(msg.?.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.empty-body");
}

test "publish and consume large message spanning multiple frames" {
    const _t = TestTimer.start("publish and consume large message spanning multiple frames"); defer _t.stop();
    const conn = try openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.queueDeclare("bunny-zig.test.large-msg", .{ .exclusive = true, .auto_delete = true });
    try ch.confirmSelect();

    // Build a message larger than the negotiated frame_max (typically 131072).
    // This forces multi-frame body encoding.
    const body_size = conn.negotiated_frame_max * 2;
    const body = try std.heap.page_allocator.alloc(u8, body_size);
    defer std.heap.page_allocator.free(body);
    @memset(body, 'A');

    try ch.publishToQueue("bunny-zig.test.large-msg", body, .{});
    try testing.expect(try ch.waitForConfirms());

    _ = try ch.basicConsume("bunny-zig.test.large-msg", "", .manual);
    const delivery = try ch.recvDelivery();
    try testing.expect(delivery != null);
    try testing.expectEqual(body_size, delivery.?.body.len);
    // Verify first and last bytes survived the multi-frame roundtrip
    try testing.expectEqual('A', delivery.?.body[0]);
    try testing.expectEqual('A', delivery.?.body[body_size - 1]);

    try ch.basicAck(delivery.?.delivery_tag, false);
    _ = try ch.queueDelete("bunny-zig.test.large-msg");
}

test "two consumers on the same queue" {
    const _t = TestTimer.start("two consumers on the same queue"); defer _t.stop();
    const conn = try openTestConnection();
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
        if (count < 4) sleepMs(25);
    }
    try testing.expectEqual(4, count);

    _ = try ch1.queueDelete("bunny-zig.test.two-consumers");
}

