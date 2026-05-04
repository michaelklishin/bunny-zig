const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "basic.ack with multiple=true acknowledges every preceding delivery" {
    const _t = h.TestTimer.start("basic.ack with multiple=true acknowledges every preceding delivery");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const q = "bunny-zig.test.ack-multiple";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "one", .{});
    try ch.publishToQueue(q, "two", .{});
    try ch.publishToQueue(q, "three", .{});
    _ = try ch.waitForConfirms();

    var last_tag: u64 = 0;
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        const got = try h.pollBasicGet(ch, q);
        try testing.expect(got != null);
        var m = got.?;
        defer m.deinit(h.test_allocator);
        last_tag = m.delivery_tag;
    }

    try ch.basicAckMultiple(last_tag);

    // Closing and redeclaring confirms the queue is fully drained.
    try testing.expect((try ch.basicGet(q, .manual)) == null);
    const info = try ch.queueDeclare(q, .{ .passive = true });
    try testing.expectEqual(@as(u32, 0), info.message_count);
}

test "basic.nack with multiple=true and requeue redelivers every preceding message" {
    const _t = h.TestTimer.start("basic.nack with multiple=true and requeue redelivers every preceding message");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const q = "bunny-zig.test.nack-multiple";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    try ch.confirmSelect();
    try ch.publishToQueue(q, "a", .{});
    try ch.publishToQueue(q, "b", .{});
    try ch.publishToQueue(q, "c", .{});
    _ = try ch.waitForConfirms();

    var last_tag: u64 = 0;
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        const got = try h.pollBasicGet(ch, q);
        try testing.expect(got != null);
        var m = got.?;
        defer m.deinit(h.test_allocator);
        last_tag = m.delivery_tag;
    }

    try ch.basicNack(last_tag, true, true);

    // After cumulative nack with requeue, every message comes back with redelivered=true.
    var seen: u32 = 0;
    while (seen < 3) : (seen += 1) {
        const got = try h.pollBasicGet(ch, q);
        try testing.expect(got != null);
        var m = got.?;
        defer m.deinit(h.test_allocator);
        try testing.expect(m.redelivered);
        try ch.basicAck(m.delivery_tag, false);
    }
}

test "basic.ack with an unknown delivery tag closes the channel" {
    const _t = h.TestTimer.start("basic.ack with an unknown delivery tag closes the channel"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    const q = "bunny-zig.test.bad-ack";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    // basic.ack is asynchronous, so the error surfaces on the next RPC.
    try ch.basicAck(999_999, false);

    const result = ch.queueDeclare(q, .{ .passive = true });
    try testing.expectError(error.PreconditionFailed, result);
    try testing.expect(!ch.isOpen());
    // The connection itself is unaffected, so a fresh channel still works.
    try testing.expect(conn.isOpen());
}

test "BasicGetResult.ack helper drains the queue" {
    const _t = h.TestTimer.start("BasicGetResult.ack helper drains the queue"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    try queue.publish("via-helper", .{});
    _ = try ch.waitForConfirms();

    const got = try h.pollBasicGet(ch, queue.name);
    try testing.expect(got != null);
    const m = got.?;
    defer m.deinit(h.test_allocator);
    try m.ack();

    const info = try ch.queueDeclare(queue.name, .{ .passive = true });
    try testing.expectEqual(@as(u32, 0), info.message_count);
}

test "BasicGetResult.rejectRequeue puts the message back" {
    const _t = h.TestTimer.start("BasicGetResult.rejectRequeue puts the message back"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    try queue.publish("retry-me", .{});
    _ = try ch.waitForConfirms();

    {
        const got = try h.pollBasicGet(ch, queue.name);
        try testing.expect(got != null);
        const m = got.?;
        defer m.deinit(h.test_allocator);
        try m.rejectRequeue();
    }

    const got2 = try h.pollBasicGet(ch, queue.name);
    try testing.expect(got2 != null);
    const m2 = got2.?;
    defer m2.deinit(h.test_allocator);
    try testing.expect(m2.redelivered);
    try m2.ack();
}

test "Channel.ackUpTo acknowledges every preceding delivery on the channel" {
    const _t = h.TestTimer.start("Channel.ackUpTo acknowledges every preceding delivery on the channel");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    try queue.publish("1", .{});
    try queue.publish("2", .{});
    try queue.publish("3", .{});
    _ = try ch.waitForConfirms();

    var last_tag: u64 = 0;
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        const got = try h.pollBasicGet(ch, queue.name);
        try testing.expect(got != null);
        const m = got.?;
        defer m.deinit(h.test_allocator);
        last_tag = m.delivery_tag;
    }

    try ch.ackUpTo(last_tag);

    const info = try ch.queueDeclare(queue.name, .{ .passive = true });
    try testing.expectEqual(@as(u32, 0), info.message_count);
}

test "Channel.nack(tag) drops a single delivery without requeueing" {
    const _t = h.TestTimer.start("Channel.nack(tag) drops a single delivery without requeueing");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    try queue.publish("drop-me", .{});
    _ = try ch.waitForConfirms();

    {
        const got = try h.pollBasicGet(ch, queue.name);
        try testing.expect(got != null);
        const m = got.?;
        defer m.deinit(h.test_allocator);
        try ch.nack(m.delivery_tag);
    }

    // The message must not come back: nack without requeueing drops it.
    try testing.expect((try ch.basicGet(queue.name, .manual)) == null);
}

test "Channel.reject(tag) drops a single delivery without requeueing" {
    const _t = h.TestTimer.start("Channel.reject(tag) drops a single delivery without requeueing");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    try queue.publish("reject-me", .{});
    _ = try ch.waitForConfirms();

    {
        const got = try h.pollBasicGet(ch, queue.name);
        try testing.expect(got != null);
        const m = got.?;
        defer m.deinit(h.test_allocator);
        try ch.reject(m.delivery_tag);
    }

    try testing.expect((try ch.basicGet(queue.name, .manual)) == null);
}

test "Delivery.ack helper acknowledges via the supplied channel" {
    const _t = h.TestTimer.start("Delivery.ack helper acknowledges via the supplied channel");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    try queue.publish("delivery-helper", .{});
    _ = try ch.waitForConfirms();

    _ = try queue.subscribe(.manual);
    const delivery = (try ch.recvDelivery()) orelse return error.NoDelivery;
    defer delivery.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "delivery-helper", delivery.body);
    try delivery.ack();

    const info = try ch.queueDeclare(queue.name, .{ .passive = true });
    try testing.expectEqual(@as(u32, 0), info.message_count);
}
