const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "dead-letter exchange receives rejected messages" {
    const _t = h.TestTimer.start("dead-letter exchange receives rejected messages");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const dlx = "bunny-zig.test.dlx";
    try ch.exchangeDeclare(dlx, bunny.ExchangeType.fanout, .{ .auto_delete = true });
    defer ch.exchangeDelete(dlx) catch {};

    const dlq = "bunny-zig.test.dlx.dlq";
    _ = try ch.queueDeclare(dlq, .{ .exclusive = true, .auto_delete = true });
    try ch.queueBind(dlq, dlx, "");

    var qa = bunny.QueueArguments{};
    defer qa.deinit(h.test_allocator);
    _ = try qa.deadLetterExchange(h.test_allocator, dlx);
    var args = try qa.build(h.test_allocator);
    defer args.deinit();

    const main_q = "bunny-zig.test.dlx.main";
    _ = try ch.queueDeclare(main_q, .{ .exclusive = true, .auto_delete = true, .arguments = args });

    try ch.confirmSelect();
    try ch.publishToQueue(main_q, "to-be-dead-lettered", .{});
    _ = try ch.waitForConfirms();

    const msg = try h.pollBasicGet(ch, main_q);
    try testing.expect(msg != null);
    try ch.basicReject(msg.?.delivery_tag, false);

    const dead = try h.pollBasicGet(ch, dlq);
    try testing.expect(dead != null);
    try testing.expectEqualSlices(u8, "to-be-dead-lettered", dead.?.body);
    // All dead-lettered messages will include the `x-death` header.
    try testing.expect(dead.?.properties.headers != null);
    try testing.expect(dead.?.properties.headers.?.get("x-death") != null);
    try ch.basicAck(dead.?.delivery_tag, false);
}
