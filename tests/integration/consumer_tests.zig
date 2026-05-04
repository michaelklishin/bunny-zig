const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "two consumers on the same queue" {
    const _t = h.TestTimer.start("two consumers on the same queue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch1 = try conn.openChannel();
    defer ch1.closeChannel() catch {};
    const ch2 = try conn.openChannel();
    defer ch2.closeChannel() catch {};

    _ = try ch1.queueDeclare("bunny-zig.test.two-consumers", .{ .exclusive = true, .auto_delete = true });

    _ = try ch1.basicConsumeWithTag("bunny-zig.test.two-consumers", "c1", .manual);
    _ = try ch2.basicConsumeWithTag("bunny-zig.test.two-consumers", "c2", .manual);

    try ch1.confirmSelect();
    for (0..4) |i| {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "msg-{d}", .{i}) catch "msg";
        try ch1.publishToQueue("bunny-zig.test.two-consumers", msg, .{});
    }
    try testing.expect(try ch1.waitForConfirms());

    // Both consumers should receive messages (round-robin).
    // Poll non-blocking to avoid deadlocking if distribution is uneven.
    var count: u32 = 0;
    for (0..200) |_| {
        if (count >= 4) break;
        if (ch1.tryRecvDelivery()) |raw| {
            var d = raw;
            defer d.deinit(h.test_allocator);
            try ch1.basicAck(d.delivery_tag, false);
            count += 1;
        }
        if (ch2.tryRecvDelivery()) |raw| {
            var d = raw;
            defer d.deinit(h.test_allocator);
            try ch2.basicAck(d.delivery_tag, false);
            count += 1;
        }
        if (count < 4) h.sleepMs(25);
    }
    try testing.expectEqual(4, count);

    _ = try ch1.queueDelete("bunny-zig.test.two-consumers");
}

test "consume with automatic ack does not require manual acknowledgement" {
    const _t = h.TestTimer.start("consume with automatic ack does not require manual acknowledgement");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.auto-ack";
    _ = try ch.queueDeclare(q, .{ .durable = true });
    defer _ = ch.queueDelete(q) catch {};

    _ = try ch.basicConsume(q, .automatic);

    try ch.confirmSelect();
    try ch.publishToQueue(q, "auto-acked", .{});
    _ = try ch.waitForConfirms();

    const got = try ch.recvDelivery();
    try testing.expect(got != null);
    var m = got.?;
    defer m.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "auto-acked", m.body);

    // No ack call. A passive redeclare reports the queue is empty because
    // the broker considers the message acknowledged the moment it was sent.
    const info = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqual(0, info.message_count);
}

test "consume returns the consumer tag the client supplied" {
    const _t = h.TestTimer.start("consume returns the consumer tag the client supplied"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.consumer-tag";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    const requested = "bunny-zig.requested-tag";
    const got = try ch.basicConsumeWithTag(q, requested, .manual);
    try testing.expectEqualSlices(u8, requested, got);
}

test "client-initiated basic.cancel stops further deliveries" {
    const _t = h.TestTimer.start("client-initiated basic.cancel stops further deliveries"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.client-cancel";
    _ = try ch.queueDeclare(q, .{ .durable = true });
    defer _ = ch.queueDelete(q) catch {};

    const tag = try ch.basicConsume(q, .manual);

    try ch.confirmSelect();
    try ch.publishToQueue(q, "before-cancel", .{});
    _ = try ch.waitForConfirms();

    const got_first = try ch.recvDelivery();
    try testing.expect(got_first != null);
    var first = got_first.?;
    defer first.deinit(h.test_allocator);
    try ch.basicAck(first.delivery_tag, false);

    try ch.basicCancel(tag);

    // After cancel, new messages are routed but not delivered to this consumer;
    // they accumulate in the queue and can be retrieved via basic.get.
    try ch.publishToQueue(q, "after-cancel", .{});
    _ = try ch.waitForConfirms();

    // Asserting against message_count is more robust than tryRecvDelivery,
    // which may race with an in-flight deliver the broker buffered before
    // processing the cancel.
    const info = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqual(@as(u32, 1), info.message_count);

    const got_via = try h.pollBasicGet(ch, q);
    try testing.expect(got_via != null);
    var via_get = got_via.?;
    defer via_get.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "after-cancel", via_get.body);
    try ch.basicAck(via_get.delivery_tag, false);
}

test "server-initiated basic.cancel fires when queue is deleted" {
    const _t = h.TestTimer.start("server-initiated basic.cancel fires when queue is deleted");
    defer _t.stop();

    const Cancel = struct {
        var fired: std.atomic.Value(u32) = .init(0);
        fn handler(_: []const u8) void {
            _ = fired.fetchAdd(1, .release);
        }
    };
    Cancel.fired.store(0, .release);

    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch_consume = try conn.openChannel();
    defer ch_consume.closeChannel() catch {};
    const ch_admin = try conn.openChannel();
    defer ch_admin.closeChannel() catch {};

    const q = "bunny-zig.test.server-cancel";
    // Exclusive queues are scoped to the connection, so a second channel on the
    // same connection may declare consumers and delete the queue.
    _ = try ch_consume.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });
    ch_consume.on_cancel = &Cancel.handler;
    _ = try ch_consume.basicConsume(q, .manual);

    _ = try ch_admin.queueDelete(q);

    var attempts: u32 = 0;
    while (Cancel.fired.load(.acquire) == 0 and attempts < 100) : (attempts += 1) h.sleepMs(20);
    try testing.expectEqual(1, Cancel.fired.load(.acquire));
}

test "client-initiated basic.cancel for an unknown consumer tag is silently accepted" {
    const _t = h.TestTimer.start("client-initiated basic.cancel for an unknown consumer tag is silently accepted");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    // RabbitMQ returns basic.cancel-ok for unknown consumer tags without raising
    // a channel exception, matching the behavior other clients rely on.
    try ch.basicCancel("never-existed");
    try testing.expect(ch.isOpen());
}

test "client-initiated basic.cancel does not requeue unacknowledged messages" {
    const _t = h.TestTimer.start("client-initiated basic.cancel does not requeue unacknowledged messages");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.cancel-no-requeue";
    // Durable, not auto_delete: cancelling the consumer would auto-delete an
    // auto_delete queue, breaking the passive declare assertion below.
    _ = try ch.queueDeclare(q, .{ .durable = true });
    defer _ = ch.queueDelete(q) catch {};

    try ch.confirmSelect();
    try ch.publishToQueue(q, "hold", .{});
    _ = try ch.waitForConfirms();

    const tag = try ch.basicConsume(q, .manual);
    const got = try ch.recvDelivery();
    try testing.expect(got != null);
    var d = got.?;
    defer d.deinit(h.test_allocator);

    try ch.basicCancel(tag);

    // Cancel does not requeue. The message stays unacked, owned by this
    // consumer's channel. queueDeclare passive shows an empty 'ready' state.
    h.sleepMs(100);
    const info = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqual(@as(u32, 0), info.message_count);

    try ch.basicAck(d.delivery_tag, false);
}

test "single-active-consumer: only one consumer receives messages at a time" {
    const _t = h.TestTimer.start("single-active-consumer: only one consumer receives messages at a time");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    const ch1 = try conn.openChannel();
    defer ch1.closeChannel() catch {};
    const ch2 = try conn.openChannel();
    defer ch2.closeChannel() catch {};

    const bunny = @import("bunny");
    var entries = [_]bunny.FieldTable.Entry{
        .{ .key = "x-single-active-consumer", .value = .{ .boolean = true } },
    };
    const args: bunny.FieldTable = .{ .entries = &entries, .allocator = undefined };

    const q = "bunny-zig.test.single-active";
    _ = try ch1.queueDeclare(q, .{ .durable = true, .arguments = args });
    defer _ = ch1.queueDelete(q) catch {};

    _ = try ch1.basicConsumeWithTag(q, "active", .manual);
    _ = try ch2.basicConsumeWithTag(q, "standby", .manual);

    try ch1.confirmSelect();
    for (0..4) |i| {
        var buf: [16]u8 = undefined;
        const body = std.fmt.bufPrint(&buf, "m-{d}", .{i}) catch "m";
        try ch1.publishToQueue(q, body, .{});
    }
    _ = try ch1.waitForConfirms();

    var active_count: u32 = 0;
    var standby_count: u32 = 0;
    for (0..80) |_| {
        if (ch1.tryRecvDelivery()) |raw| {
            var d = raw;
            defer d.deinit(h.test_allocator);
            try ch1.basicAck(d.delivery_tag, false);
            active_count += 1;
        }
        if (ch2.tryRecvDelivery()) |raw| {
            var d = raw;
            defer d.deinit(h.test_allocator);
            try ch2.basicAck(d.delivery_tag, false);
            standby_count += 1;
        }
        if (active_count + standby_count >= 4) break;
        h.sleepMs(25);
    }
    try testing.expectEqual(@as(u32, 4), active_count);
    try testing.expectEqual(@as(u32, 0), standby_count);
}

