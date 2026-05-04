const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// Direct Reply-to is a RabbitMQ feature that uses `amq.rabbitmq.reply-to`, a
// pseudo-queue that lets a request-reply server publish responses directly
// onto the client's connection without the client declaring a reply queue
// first. The client publishes a request with `reply_to = "amq.rabbitmq.reply-to"`
// and a `correlation_id`; the request-reply server publishes the response to
// the default exchange with the routing key taken from `reply_to`. See
// https://www.rabbitmq.com/docs/direct-reply-to.

test "direct reply-to: RPC roundtrip via amq.rabbitmq.reply-to" {
    const _t = h.TestTimer.start("direct reply-to: RPC roundtrip via amq.rabbitmq.reply-to");
    defer _t.stop();

    const conn = try h.openTestConnection();
    defer conn.deinit();

    const server_ch = try conn.openChannel();
    defer server_ch.close();
    const client_ch = try conn.openChannel();
    defer client_ch.close();

    const rpc_queue = "bunny-zig.test.rpc-server";
    _ = try server_ch.queueDeclare(rpc_queue, .{ .exclusive = true, .auto_delete = true });
    _ = try server_ch.basicConsumeWithTag(rpc_queue, "rpc-server", .automatic);

    // Client subscribes to the pseudo-queue. Auto-ack is required by the broker.
    _ = try client_ch.basicConsumeWithTag("amq.rabbitmq.reply-to", "rpc-client", .automatic);

    try client_ch.publish("ping", .{
        .exchange = "",
        .routing_key = rpc_queue,
        .properties = .{
            .reply_to = "amq.rabbitmq.reply-to",
            .correlation_id = "req-1",
        },
    });

    // Server receives the request and replies to the client's pseudo-queue.
    const got_req = try server_ch.recvDelivery();
    try testing.expect(got_req != null);
    var req = got_req.?;
    defer req.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "ping", req.body);
    const reply_to = req.properties.reply_to orelse return error.MissingReplyTo;
    const correlation_id = req.properties.correlation_id orelse return error.MissingCorrelationId;

    try server_ch.publish("pong", .{
        .exchange = "",
        .routing_key = reply_to,
        .properties = .{ .correlation_id = correlation_id },
    });

    const got_reply = try client_ch.recvDelivery();
    try testing.expect(got_reply != null);
    var reply = got_reply.?;
    defer reply.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "pong", reply.body);
    try testing.expectEqualSlices(u8, "req-1", reply.properties.correlation_id orelse "");
}

// Same flow as above, but the request-reply server replies via `Delivery.respond`, the
// helper that propagates `correlation_id` and publishes a response to `delivery.reply_to` via the default exchange.
test "direct reply-to: Delivery.respond roundtrip" {
    const _t = h.TestTimer.start("direct reply-to: Delivery.respond roundtrip");
    defer _t.stop();

    const conn = try h.openTestConnection();
    defer conn.deinit();

    const server_ch = try conn.openChannel();
    defer server_ch.close();
    const client_ch = try conn.openChannel();
    defer client_ch.close();

    const rpc_queue = "bunny-zig.test.rpc-server-respond";
    _ = try server_ch.queueDeclare(rpc_queue, .{ .exclusive = true, .auto_delete = true });
    _ = try server_ch.basicConsumeWithTag(rpc_queue, "rpc-server-respond", .automatic);
    _ = try client_ch.basicConsumeWithTag("amq.rabbitmq.reply-to", "rpc-client-respond", .automatic);

    try client_ch.publish("ping", .{
        .exchange = "",
        .routing_key = rpc_queue,
        .properties = .{
            .reply_to = "amq.rabbitmq.reply-to",
            .correlation_id = "req-2",
        },
    });

    const req = (try server_ch.recvDelivery()) orelse return error.NoRequest;
    defer req.deinit(h.test_allocator);
    try req.respond("pong");

    const reply = (try client_ch.recvDelivery()) orelse return error.NoReply;
    defer reply.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "pong", reply.body);
    try testing.expectEqualSlices(u8, "req-2", reply.properties.correlation_id orelse "");
}

test "Channel.respondTo returns NoReplyTo when delivery has no reply_to" {
    const _t = h.TestTimer.start("Channel.respondTo returns NoReplyTo when delivery has no reply_to");
    defer _t.stop();

    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    try queue.publish("no-reply", .{});
    _ = try ch.waitForConfirms();

    _ = try queue.subscribe(.automatic);
    const req = (try ch.recvDelivery()) orelse return error.NoRequest;
    defer req.deinit(h.test_allocator);
    try testing.expectError(error.NoReplyTo, ch.respondTo(req, "anything"));
}
