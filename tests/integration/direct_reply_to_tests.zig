const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// amq.rabbitmq.reply-to is a pseudo-queue that lets a server consume directly
// from the requester's connection without declaring a queue first. It enables
// the classic RPC pattern: requester sets reply_to and correlation_id, the
// responder publishes to amq.direct with the routing key from reply_to.

test "direct reply-to: RPC roundtrip via amq.rabbitmq.reply-to" {
    const _t = h.TestTimer.start("direct reply-to: RPC roundtrip via amq.rabbitmq.reply-to");
    defer _t.stop();

    const conn = try h.openTestConnection();
    defer conn.deinit();

    const server_ch = try conn.openChannel();
    defer server_ch.closeChannel() catch {};
    const client_ch = try conn.openChannel();
    defer client_ch.closeChannel() catch {};

    const rpc_queue = "bunny-zig.test.rpc-server";
    _ = try server_ch.queueDeclare(rpc_queue, .{ .exclusive = true, .auto_delete = true });
    _ = try server_ch.basicConsume(rpc_queue, "rpc-server", .automatic);

    // Client subscribes to the pseudo-queue. Auto-ack is required by the broker.
    _ = try client_ch.basicConsume("amq.rabbitmq.reply-to", "rpc-client", .automatic);

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
