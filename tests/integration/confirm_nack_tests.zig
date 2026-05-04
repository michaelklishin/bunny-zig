const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "publisher confirms: broker nacks a publish that overflows reject-publish" {
    const _t = h.TestTimer.start("publisher confirms: broker nacks a publish that overflows reject-publish");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var qa = bunny.QueueArguments{};
    defer qa.deinit(h.test_allocator);
    _ = try qa.maxLength(h.test_allocator, 1);
    _ = try qa.overflow(h.test_allocator, .reject_publish);
    var args = try qa.build(h.test_allocator);
    defer args.deinit();

    const q = "bunny-zig.test.reject-publish";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true, .arguments = args });

    try ch.confirmSelectWithOptions(.{ .tracking = true });

    try ch.publishToQueue(q, "first", .{});
    // The queue is full and configured to reject-publish, so the broker
    // sends basic.nack which the client surfaces as PublishNacked.
    const second = ch.publishToQueue(q, "second", .{});
    try testing.expectError(error.PublishNacked, second);
}

test "publisher confirms: per-promise resolution distinguishes ack from nack" {
    const _t = h.TestTimer.start("publisher confirms: per-promise resolution distinguishes ack from nack");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var qa = bunny.QueueArguments{};
    defer qa.deinit(h.test_allocator);
    _ = try qa.maxLength(h.test_allocator, 1);
    _ = try qa.overflow(h.test_allocator, .reject_publish);
    var args = try qa.build(h.test_allocator);
    defer args.deinit();

    const q = "bunny-zig.test.confirm-promises";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true, .arguments = args });

    try ch.confirmSelectWithOptions(.{ .tracking = true });

    const p_ok = (try ch.publishAsync("accepted", .{ .routing_key = q })).?;
    defer ch.releasePromise(p_ok);
    try testing.expectEqual(bunny.ConfirmPromise.ConfirmResult.acked, p_ok.wait());

    const p_nack = (try ch.publishAsync("rejected", .{ .routing_key = q })).?;
    defer ch.releasePromise(p_nack);
    try testing.expectEqual(bunny.ConfirmPromise.ConfirmResult.nacked, p_nack.wait());
}
