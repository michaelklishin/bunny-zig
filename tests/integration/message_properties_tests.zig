const std = @import("std");
const h = @import("test_helpers.zig");
const testing = h.testing;
const BasicProperties = h.BasicProperties;

test "publish with properties" {
    const _t = h.TestTimer.start("publish with properties"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);
    try ch.confirmSelect();

    const props = BasicProperties.default
        .withContentType("application/json")
        .asPersistent()
        .withMessageId("msg-001")
        .withCorrelationId("corr-001")
        .withAppId("bunny-zig-test");

    try queue.publish("{\"key\": \"value\"}", props);
    try testing.expect(try ch.waitForConfirms());

    const result = try queue.get(.manual);
    try testing.expect(result != null);
    const msg = result.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqualSlices(u8, "application/json", msg.properties.content_type.?);
    try testing.expectEqual(2, msg.properties.delivery_mode.?);
    try testing.expectEqualSlices(u8, "msg-001", msg.properties.message_id.?);

    try msg.ack();
}

test "persistent delivery_mode survives publish to consume roundtrip" {
    const _t = h.TestTimer.start("persistent delivery_mode survives publish to consume roundtrip");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    try ch.confirmSelect();
    try queue.publish("durable payload", .{ .delivery_mode = 2 });
    _ = try ch.waitForConfirms();

    const got = try h.pollBasicGet(ch, queue.name);
    try testing.expect(got != null);
    const msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqual(@as(?u8, 2), msg.properties.delivery_mode);
    try msg.ack();
}

test "basic properties round-trip through publish and basic.get" {
    const _t = h.TestTimer.start("basic properties round-trip through publish and basic.get");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.close();

    var queue = try ch.temporaryQueue();
    defer queue.deinit(h.test_allocator);

    const props = BasicProperties{
        .content_type = "application/json",
        .content_encoding = "utf-8",
        .delivery_mode = 2,
        .priority = 5,
        .correlation_id = "corr-123",
        .reply_to = "reply.queue",
        .expiration = "60000",
        .message_id = "msg-abc",
        .timestamp = 1_700_000_000,
        .app_id = "bunny-zig-tests",
    };

    try ch.confirmSelect();
    try queue.publish("props body", props);
    _ = try ch.waitForConfirms();

    const got_msg = try h.pollBasicGet(ch, queue.name);
    try testing.expect(got_msg != null);
    const msg = got_msg.?;
    defer msg.deinit(h.test_allocator);
    const got = msg.properties;
    try testing.expectEqualSlices(u8, "application/json", got.content_type.?);
    try testing.expectEqualSlices(u8, "utf-8", got.content_encoding.?);
    try testing.expectEqual(@as(?u8, 2), got.delivery_mode);
    try testing.expectEqual(@as(?u8, 5), got.priority);
    try testing.expectEqualSlices(u8, "corr-123", got.correlation_id.?);
    try testing.expectEqualSlices(u8, "reply.queue", got.reply_to.?);
    try testing.expectEqualSlices(u8, "60000", got.expiration.?);
    try testing.expectEqualSlices(u8, "msg-abc", got.message_id.?);
    try testing.expectEqual(@as(?u64, 1_700_000_000), got.timestamp);
    try testing.expectEqualSlices(u8, "bunny-zig-tests", got.app_id.?);

    try msg.ack();
}
