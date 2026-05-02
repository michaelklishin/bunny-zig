const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "bind and unbind a queue" {
    const _t = h.TestTimer.start("bind and unbind a queue"); defer _t.stop();
    const conn = try h.openTestConnection();
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
    const _t = h.TestTimer.start("exchange-to-exchange binding"); defer _t.stop();
    const conn = try h.openTestConnection();
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

test "auto-delete source exchange is removed when its last binding goes away" {
    const _t = h.TestTimer.start("auto-delete source exchange is removed when its last binding goes away");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const src = "bunny-zig.test.auto-delete-source";
    const dst = "bunny-zig.test.auto-delete-dest";
    try ch.exchangeDeclare(src, bunny.ExchangeType.fanout, .{ .auto_delete = true });
    try ch.exchangeDeclare(dst, bunny.ExchangeType.fanout, .{ .auto_delete = true });
    defer ch.exchangeDelete(dst) catch {};

    try ch.exchangeBind(dst, src, "");
    try ch.exchangeUnbind(dst, src, "");

    // Once the source's only binding is gone, the broker auto-deletes it, so a
    // passive declare reports NOT_FOUND.
    const ch2 = try conn.openChannel();
    defer ch2.closeChannel() catch {};
    const result = ch2.exchangeDeclare(src, bunny.ExchangeType.fanout, .{ .passive = true });
    try testing.expectError(error.NotFound, result);
}
