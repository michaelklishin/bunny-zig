const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "declare and delete a fanout exchange" {
    const _t = h.TestTimer.start("declare and delete a fanout exchange"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    try ch.declareFanout("bunny-zig.test.fanout");
    try ch.exchangeDelete("bunny-zig.test.fanout");
}

test "declare and delete a topic exchange" {
    const _t = h.TestTimer.start("declare and delete a topic exchange"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    try ch.declareTopic("bunny-zig.test.topic");
    try ch.exchangeDelete("bunny-zig.test.topic");
}

test "declare and delete a direct exchange" {
    const _t = h.TestTimer.start("declare and delete a direct exchange"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    try ch.declareDirect("bunny-zig.test.direct");
    try ch.exchangeDelete("bunny-zig.test.direct");
}

test "declare and delete a headers exchange" {
    const _t = h.TestTimer.start("declare and delete a headers exchange"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    try ch.declareHeaders("bunny-zig.test.headers");
    try ch.exchangeDelete("bunny-zig.test.headers");
}

test "passive declare of amq.direct succeeds" {
    const _t = h.TestTimer.start("passive declare of amq.direct succeeds"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    try ch.exchangeDeclare("amq.direct", "direct", .{ .passive = true });
}

test "passive declare of a missing exchange closes the channel" {
    const _t = h.TestTimer.start("passive declare of a missing exchange closes the channel"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const result = ch.exchangeDeclare("bunny-zig.test.no-such-exchange", "direct", .{ .passive = true });
    try testing.expectError(error.NotFound, result);
    try testing.expect(!ch.isOpen());
}

test "exchangeDeclarePassive convenience helper asserts a built-in exchange" {
    const _t = h.TestTimer.start("exchangeDeclarePassive convenience helper asserts a built-in exchange"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    try ch.exchangeDeclarePassive("amq.fanout");
}

test "exchangeDeclarePassive on a missing exchange closes the channel" {
    const _t = h.TestTimer.start("exchangeDeclarePassive on a missing exchange closes the channel"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const result = ch.exchangeDeclarePassive("bunny-zig.test.passive-helper-no-such-exchange");
    try testing.expectError(error.NotFound, result);
    try testing.expect(!ch.isOpen());
}

test "exchange.delete with if_unused=true fails when bindings exist" {
    const _t = h.TestTimer.start("exchange.delete with if_unused=true fails when bindings exist"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const ex = "bunny-zig.test.if-unused";
    try ch.exchangeDeclare(ex, "direct", .{ .auto_delete = true });

    const q = "bunny-zig.test.if-unused-q";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });
    try ch.queueBind(q, ex, "k");

    // While the binding exists, exchange.delete with if_unused=true must
    // fail with PRECONDITION_FAILED (406), which closes the channel.
    const result = ch.exchangeDeleteWithOptions(ex, true);
    try testing.expectError(error.PreconditionFailed, result);
}

test "exchange-to-exchange unbind stops routing across the binding" {
    const _t = h.TestTimer.start("exchange-to-exchange unbind stops routing across the binding"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const src = "bunny-zig.test.e2e-unbind-src";
    const dest = "bunny-zig.test.e2e-unbind-dest";
    // Cannot use auto_delete here: removing the dest -> src binding would
    // leave src bindingless and the broker would auto-delete it before the
    // second publish lands.
    try ch.exchangeDeclare(src, "fanout", .{});
    defer ch.exchangeDelete(src) catch {};
    try ch.exchangeDeclare(dest, "fanout", .{});
    defer ch.exchangeDelete(dest) catch {};

    try ch.exchangeBind(dest, src, "");

    const q = "bunny-zig.test.e2e-unbind-q";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });
    try ch.queueBind(q, dest, "");

    try ch.confirmSelect();
    try ch.publish("before-unbind", .{ .exchange = src });
    _ = try ch.waitForConfirms();

    const got_before = try h.pollBasicGet(ch, q);
    try testing.expect(got_before != null);
    var before = got_before.?;
    defer before.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "before-unbind", before.body);
    try ch.basicAck(before.delivery_tag, false);

    try ch.exchangeUnbind(dest, src, "");

    try ch.publish("after-unbind", .{ .exchange = src });
    _ = try ch.waitForConfirms();

    // The binding is gone, so the dest exchange no longer receives anything.
    const info = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqual(@as(u32, 0), info.message_count);
}

test "publishing to a predeclared amq.fanout exchange routes to bound queue" {
    const _t = h.TestTimer.start("publishing to a predeclared amq.fanout exchange routes to bound queue");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const q = "bunny-zig.test.amq-fanout";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });
    try ch.queueBind(q, "amq.fanout", "");

    try ch.confirmSelect();
    try ch.publish("via amq.fanout", .{ .exchange = "amq.fanout" });
    _ = try ch.waitForConfirms();

    const got = try h.pollBasicGet(ch, q);
    try testing.expect(got != null);
    var msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "via amq.fanout", msg.body);
    try ch.basicAck(msg.delivery_tag, false);
}
