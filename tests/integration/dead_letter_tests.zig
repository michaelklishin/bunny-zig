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
    defer ch.close();

    const dlx = "bunny-zig.test.dlx";
    _ = try ch.exchangeDeclare(dlx, bunny.ExchangeType.fanout, .{ .auto_delete = true });
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

    const got_msg = try h.pollBasicGet(ch, main_q);
    try testing.expect(got_msg != null);
    var msg = got_msg.?;
    defer msg.deinit(h.test_allocator);
    try ch.basicReject(msg.delivery_tag, false);

    const got_dead = try h.pollBasicGet(ch, dlq);
    try testing.expect(got_dead != null);
    var dead = got_dead.?;
    defer dead.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "to-be-dead-lettered", dead.body);
    // All dead-lettered messages will include the `x-death` header.
    try testing.expect(dead.properties.headers != null);
    try testing.expect(dead.properties.headers.?.get("x-death") != null);
    try ch.basicAck(dead.delivery_tag, false);
}

test "dead-letter exchange respects x-dead-letter-routing-key override" {
    const _t = h.TestTimer.start("dead-letter exchange respects x-dead-letter-routing-key override");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const dlx = "bunny-zig.test.dlx-routed";
    _ = try ch.exchangeDeclare(dlx, bunny.ExchangeType.direct, .{ .auto_delete = true });
    defer ch.exchangeDelete(dlx) catch {};

    const dlq = "bunny-zig.test.dlx-routed.dlq";
    _ = try ch.queueDeclare(dlq, .{ .exclusive = true, .auto_delete = true });
    // The DLQ binding listens on the override key; the original publish uses
    // a different key, so dead-lettering only routes if the override applies.
    try ch.queueBind(dlq, dlx, "dead.route");

    var qa = bunny.QueueArguments{};
    defer qa.deinit(h.test_allocator);
    _ = try qa.deadLetterExchange(h.test_allocator, dlx);
    _ = try qa.deadLetterRoutingKey(h.test_allocator, "dead.route");
    var args = try qa.build(h.test_allocator);
    defer args.deinit();

    const main_q = "bunny-zig.test.dlx-routed.main";
    _ = try ch.queueDeclare(main_q, .{ .exclusive = true, .auto_delete = true, .arguments = args });

    try ch.confirmSelect();
    try ch.publish("override-routed", .{ .exchange = "", .routing_key = main_q });
    _ = try ch.waitForConfirms();

    const got_msg = try h.pollBasicGet(ch, main_q);
    try testing.expect(got_msg != null);
    var msg = got_msg.?;
    defer msg.deinit(h.test_allocator);
    try ch.basicReject(msg.delivery_tag, false);

    const got_dead = try h.pollBasicGet(ch, dlq);
    try testing.expect(got_dead != null);
    var dead = got_dead.?;
    defer dead.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "override-routed", dead.body);
    try ch.basicAck(dead.delivery_tag, false);
}

test "x-death header is bounded by distinct (queue, reason) pairs across cycles" {
    const _t = h.TestTimer.start("x-death header is bounded by distinct (queue, reason) pairs across cycles");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    // Two queues each dead-letter to the other's exchange; bouncing rejects
    // back and forth produces exactly two x-death entries (one per queue),
    // and the broker increments the matching entry's count rather than
    // appending a new one each cycle.
    const ex_a = "bunny-zig.test.x-death.ex-a";
    const ex_b = "bunny-zig.test.x-death.ex-b";
    _ = try ch.exchangeDeclare(ex_a, bunny.ExchangeType.fanout, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex_a) catch {};
    _ = try ch.exchangeDeclare(ex_b, bunny.ExchangeType.fanout, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex_b) catch {};

    var qa_a = bunny.QueueArguments{};
    defer qa_a.deinit(h.test_allocator);
    _ = try qa_a.deadLetterExchange(h.test_allocator, ex_b);
    var args_a = try qa_a.build(h.test_allocator);
    defer args_a.deinit();

    var qa_b = bunny.QueueArguments{};
    defer qa_b.deinit(h.test_allocator);
    _ = try qa_b.deadLetterExchange(h.test_allocator, ex_a);
    var args_b = try qa_b.build(h.test_allocator);
    defer args_b.deinit();

    const q_a = "bunny-zig.test.x-death.q-a";
    const q_b = "bunny-zig.test.x-death.q-b";
    _ = try ch.queueDeclare(q_a, .{ .exclusive = true, .auto_delete = true, .arguments = args_a });
    _ = try ch.queueDeclare(q_b, .{ .exclusive = true, .auto_delete = true, .arguments = args_b });
    try ch.queueBind(q_a, ex_a, "");
    try ch.queueBind(q_b, ex_b, "");

    try ch.confirmSelect();
    try ch.publish("bouncer", .{ .exchange = ex_a });
    _ = try ch.waitForConfirms();

    // Bounce four times: q_a -> q_b -> q_a -> q_b -> q_a.
    const queues: [4][]const u8 = .{ q_a, q_b, q_a, q_b };
    for (queues) |q| {
        const got_m = try h.pollBasicGet(ch, q);
        try testing.expect(got_m != null);
        var m = got_m.?;
        defer m.deinit(h.test_allocator);
        try ch.basicReject(m.delivery_tag, false);
    }

    const got_final = try h.pollBasicGet(ch, q_a);
    try testing.expect(got_final != null);
    var final = got_final.?;
    defer final.deinit(h.test_allocator);
    try testing.expect(final.properties.headers != null);
    const x_death = final.properties.headers.?.get("x-death");
    try testing.expect(x_death != null);
    switch (x_death.?) {
        .array => |items| try testing.expectEqual(@as(usize, 2), items.len),
        else => return error.TestExpectedArray,
    }
    try ch.basicAck(final.delivery_tag, false);
}
