const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "declare and delete a fanout exchange" {
    const _t = h.TestTimer.start("declare and delete a fanout exchange"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareFanout("bunny-zig.test.fanout");
    try ch.exchangeDelete("bunny-zig.test.fanout");
}

test "declare and delete a topic exchange" {
    const _t = h.TestTimer.start("declare and delete a topic exchange"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareTopic("bunny-zig.test.topic");
    try ch.exchangeDelete("bunny-zig.test.topic");
}

test "declare and delete a direct exchange" {
    const _t = h.TestTimer.start("declare and delete a direct exchange"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareDirect("bunny-zig.test.direct");
    try ch.exchangeDelete("bunny-zig.test.direct");
}

test "declare and delete a headers exchange" {
    const _t = h.TestTimer.start("declare and delete a headers exchange"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    try ch.declareHeaders("bunny-zig.test.headers");
    try ch.exchangeDelete("bunny-zig.test.headers");
}

test "publishing to a predeclared amq.fanout exchange routes to bound queue" {
    const _t = h.TestTimer.start("publishing to a predeclared amq.fanout exchange routes to bound queue");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.amq-fanout";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });
    try ch.queueBind(q, "amq.fanout", "");

    try ch.confirmSelect();
    try ch.publish("via amq.fanout", .{ .exchange = "amq.fanout" });
    _ = try ch.waitForConfirms();

    const msg = try h.pollBasicGet(ch, q);
    try testing.expect(msg != null);
    try testing.expectEqualSlices(u8, "via amq.fanout", msg.?.body);
    try ch.basicAck(msg.?.delivery_tag, false);
}
