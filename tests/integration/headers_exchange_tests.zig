const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "headers exchange routes when x-match=any matches a single header" {
    const _t = h.TestTimer.start("headers exchange routes when x-match=any matches a single header");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const ex = "bunny-zig.test.headers-any";
    try ch.exchangeDeclare(ex, bunny.ExchangeType.headers, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex) catch {};

    const q = "bunny-zig.test.headers-any.q";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    var bind_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "x-match", .value = .{ .long_string = "any" } },
        .{ .key = "format", .value = .{ .long_string = "pdf" } },
        .{ .key = "type", .value = .{ .long_string = "report" } },
    };
    try ch.queueBindWithArgs(q, ex, "", .{ .entries = &bind_entries, .allocator = undefined });

    var header_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "format", .value = .{ .long_string = "pdf" } },
    };
    try ch.confirmSelect();
    try ch.publish("matched", .{
        .exchange = ex,
        .properties = .{ .headers = .{ .entries = &header_entries, .allocator = undefined } },
    });
    _ = try ch.waitForConfirms();

    const got = try h.pollBasicGet(ch, q);
    try testing.expect(got != null);
    var msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "matched", msg.body);
    try ch.basicAck(msg.delivery_tag, false);
}

test "headers exchange does not route when x-match=all is not satisfied" {
    const _t = h.TestTimer.start("headers exchange does not route when x-match=all is not satisfied");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const ex = "bunny-zig.test.headers-all";
    try ch.exchangeDeclare(ex, bunny.ExchangeType.headers, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex) catch {};

    const q_strict = "bunny-zig.test.headers-all.strict";
    _ = try ch.queueDeclare(q_strict, .{ .exclusive = true, .auto_delete = true });
    const q_control = "bunny-zig.test.headers-all.control";
    _ = try ch.queueDeclare(q_control, .{ .exclusive = true, .auto_delete = true });

    var strict_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "x-match", .value = .{ .long_string = "all" } },
        .{ .key = "format", .value = .{ .long_string = "pdf" } },
        .{ .key = "type", .value = .{ .long_string = "report" } },
    };
    try ch.queueBindWithArgs(q_strict, ex, "", .{ .entries = &strict_entries, .allocator = undefined });

    var control_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "x-match", .value = .{ .long_string = "any" } },
        .{ .key = "format", .value = .{ .long_string = "pdf" } },
    };
    try ch.queueBindWithArgs(q_control, ex, "", .{ .entries = &control_entries, .allocator = undefined });

    // Only one of the two required headers is present, so routing to q_strict must miss
    // while routing to the control queue must hit, proving the publish path is healthy.
    var header_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "format", .value = .{ .long_string = "pdf" } },
    };
    try ch.confirmSelect();
    try ch.publish("not-matched", .{
        .exchange = ex,
        .properties = .{ .headers = .{ .entries = &header_entries, .allocator = undefined } },
    });
    _ = try ch.waitForConfirms();

    const got_control = try h.pollBasicGet(ch, q_control);
    try testing.expect(got_control != null);
    var control_msg = got_control.?;
    defer control_msg.deinit(h.test_allocator);
    try ch.basicAck(control_msg.delivery_tag, false);

    const strict_msg = try ch.basicGet(q_strict, .manual);
    try testing.expect(strict_msg == null);
}

test "headers exchange routes when bound on heterogeneous header types" {
    const _t = h.TestTimer.start("headers exchange routes when bound on heterogeneous header types");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const ex = "bunny-zig.test.headers-types";
    try ch.exchangeDeclare(ex, bunny.ExchangeType.headers, .{ .auto_delete = true });
    defer ch.exchangeDelete(ex) catch {};

    const q = "bunny-zig.test.headers-types.q";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    // The binding requires three headers of different AMQP types: string, int,
    // and bool. The published headers must match all three for x-match=all.
    var bind_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "x-match", .value = .{ .long_string = "all" } },
        .{ .key = "kind", .value = .{ .long_string = "report" } },
        .{ .key = "version", .value = .{ .i32 = 7 } },
        .{ .key = "approved", .value = .{ .boolean = true } },
    };
    try ch.queueBindWithArgs(q, ex, "", .{ .entries = &bind_entries, .allocator = undefined });

    var header_entries = [_]bunny.FieldTable.Entry{
        .{ .key = "kind", .value = .{ .long_string = "report" } },
        .{ .key = "version", .value = .{ .i32 = 7 } },
        .{ .key = "approved", .value = .{ .boolean = true } },
        .{ .key = "extra", .value = .{ .timestamp = 1700000000 } },
    };
    try ch.confirmSelect();
    try ch.publish("typed-match", .{
        .exchange = ex,
        .properties = .{ .headers = .{ .entries = &header_entries, .allocator = undefined } },
    });
    _ = try ch.waitForConfirms();

    const got = try h.pollBasicGet(ch, q);
    try testing.expect(got != null);
    var msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "typed-match", msg.body);
    try ch.basicAck(msg.delivery_tag, false);
}
