const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// Smoke coverage for the auto-tag overloads added on Channel and Queue. The
// pull-API overloads are already exercised by nearly every consume test; these
// pin the callback and args variants so they cannot rot.

test "Channel.basicConsumeWith (auto tag) delivers via callback" {
    const _t = h.TestTimer.start("Channel.basicConsumeWith (auto tag) delivers via callback");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const Hits = struct {
        var count: std.atomic.Value(u32) = .init(0);
        fn handler(_: bunny.Delivery) void {
            _ = count.fetchAdd(1, .release);
        }
    };
    Hits.count.store(0, .release);

    const q = "bunny-zig.test.consume-with-auto-tag";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });
    const tag = try ch.basicConsumeWith(q, .automatic, &Hits.handler);
    try testing.expect(tag.len > 0);
    h.sleepMs(50);

    try ch.confirmSelect();
    try ch.publishToQueue(q, "x", .{});
    _ = try ch.waitForConfirms();

    var attempts: u32 = 0;
    while (Hits.count.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(10);
    try testing.expectEqual(@as(u32, 1), Hits.count.load(.acquire));
}

test "Channel.basicConsumeWithArgs (auto tag) accepts custom arguments" {
    const _t = h.TestTimer.start("Channel.basicConsumeWithArgs (auto tag) accepts custom arguments");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    _ = try ch.streamQueue("bunny-zig.test.consume-args-auto-tag");
    defer _ = ch.queueDelete("bunny-zig.test.consume-args-auto-tag") catch {};

    const args = try bunny.FieldTable.fromEntries(h.test_allocator, &.{
        .{ .key = "x-stream-offset", .value = .{ .long_string = "first" } },
    });
    defer {
        var a = args;
        a.deinit();
    }

    try ch.basicQos(10, false);
    const tag = try ch.basicConsumeWithArgs("bunny-zig.test.consume-args-auto-tag", .manual, false, args);
    try testing.expect(tag.len > 0);
}

test "Queue.subscribe (auto tag) returns a server-generated consumer tag" {
    const _t = h.TestTimer.start("Queue.subscribe (auto tag) returns a server-generated consumer tag");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    var q = try ch.declareQueueHandle("bunny-zig.test.queue-subscribe-auto", .{ .exclusive = true, .auto_delete = true });
    defer q.deinit(h.test_allocator);
    const tag = try q.subscribe(.manual);
    try testing.expect(tag.len > 0);
}

test "Queue.subscribeWith (auto tag) delivers via callback" {
    const _t = h.TestTimer.start("Queue.subscribeWith (auto tag) delivers via callback");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const Hits = struct {
        var count: std.atomic.Value(u32) = .init(0);
        fn handler(_: bunny.Delivery) void {
            _ = count.fetchAdd(1, .release);
        }
    };
    Hits.count.store(0, .release);

    const queue_name = "bunny-zig.test.queue-subscribe-with-auto";
    var q = try ch.declareQueueHandle(queue_name, .{ .exclusive = true, .auto_delete = true });
    defer q.deinit(h.test_allocator);
    const tag = try q.subscribeWith(.automatic, &Hits.handler);
    try testing.expect(tag.len > 0);
    // Let the callback register before the broker can deliver.
    h.sleepMs(50);

    try ch.confirmSelect();
    // Uses a well known name. Use `Queue.name` with server-named queues.
    try ch.publishToQueue(queue_name, "y", .{});
    _ = try ch.waitForConfirms();

    var attempts: u32 = 0;
    while (Hits.count.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(10);
    try testing.expectEqual(@as(u32, 1), Hits.count.load(.acquire));
}

test "Queue.subscribeWithTag honors the explicit tag" {
    const _t = h.TestTimer.start("Queue.subscribeWithTag honors the explicit tag");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    var q = try ch.declareQueueHandle("bunny-zig.test.queue-subscribe-with-tag", .{ .exclusive = true, .auto_delete = true });
    defer q.deinit(h.test_allocator);
    const tag = try q.subscribeWithTag("explicit-tag", .manual);
    try testing.expectEqualSlices(u8, "explicit-tag", tag);
}
