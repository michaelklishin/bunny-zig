const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "alternate-exchange catches messages that would otherwise be unrouted" {
    const _t = h.TestTimer.start("alternate-exchange catches messages that would otherwise be unrouted");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const ae = "bunny-zig.test.ae.alternate";
    try ch.exchangeDeclare(ae, bunny.ExchangeType.fanout, .{ .auto_delete = true });
    defer ch.exchangeDelete(ae) catch {};

    const ae_q = "bunny-zig.test.ae.catchall";
    _ = try ch.queueDeclare(ae_q, .{ .exclusive = true, .auto_delete = true });
    try ch.queueBind(ae_q, ae, "");

    // Build the x-alternate-exchange argument table for the primary exchange.
    var ex_args = [_]bunny.FieldTable.Entry{
        .{ .key = "alternate-exchange", .value = .{ .long_string = ae } },
    };
    const args: bunny.FieldTable = .{ .entries = &ex_args, .allocator = undefined };

    const primary = "bunny-zig.test.ae.primary";
    try ch.exchangeDeclare(primary, bunny.ExchangeType.direct, .{ .auto_delete = true, .arguments = args });
    defer ch.exchangeDelete(primary) catch {};

    try ch.confirmSelect();
    try ch.publish("orphan", .{ .exchange = primary, .routing_key = "no.binding" });
    _ = try ch.waitForConfirms();

    const got = try h.pollBasicGet(ch, ae_q);
    try testing.expect(got != null);
    var msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "orphan", msg.body);
    try ch.basicAck(msg.delivery_tag, false);
}
