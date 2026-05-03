const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// Frame-max negotiation determines how a message body is split into body
// frames on the wire. The broker advertises a maximum, the client requests
// its own maximum, and the smaller of the two wins.

test "frame_max: negotiated value is non-zero and at most the client request" {
    const _t = h.TestTimer.start("frame_max: negotiated value is non-zero and at most the client request");
    defer _t.stop();
    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .frame_max = 16_384,
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    try testing.expect(conn.negotiated_frame_max > 0);
    try testing.expect(conn.negotiated_frame_max <= 16_384);
}

test "frame_max: a body larger than frame_max is split across multiple body frames" {
    const _t = h.TestTimer.start("frame_max: a body larger than frame_max is split across multiple body frames");
    defer _t.stop();
    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .frame_max = 8192,
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.frame-max-split";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    // 64 KB is well above any reasonable frame_max, forcing the body to be
    // chunked. The published bytes must round-trip exactly.
    var body: [65_536]u8 = undefined;
    for (&body, 0..) |*b, i| b.* = @intCast(i & 0xFF);

    try ch.confirmSelect();
    try ch.publishToQueue(q, &body, .{});
    _ = try ch.waitForConfirms();

    const got = try h.pollBasicGet(ch, q);
    try testing.expect(got != null);
    var msg = got.?;
    defer msg.deinit(h.test_allocator);
    try testing.expectEqual(@as(usize, body.len), msg.body.len);
    try testing.expectEqualSlices(u8, &body, msg.body);
    try ch.basicAck(msg.delivery_tag, false);
}

test "frame_max: client value above the client buffer cap is rejected" {
    const _t = h.TestTimer.start("frame_max: client value above the client buffer cap is rejected");
    defer _t.stop();
    const result = bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .frame_max = 1_000_000,
        .recovery = .{ .enabled = false },
    });
    try testing.expectError(error.FrameMaxTooLarge, result);
}

test "frame_max: client value below the protocol minimum (4096) is rejected" {
    const _t = h.TestTimer.start("frame_max: client value below the protocol minimum (4096) is rejected");
    defer _t.stop();
    const result = bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .frame_max = 1024,
        .recovery = .{ .enabled = false },
    });
    try testing.expectError(error.FrameMaxTooSmall, result);
}

// AMQP 0-9-1 frame overhead is 8 bytes (1 type + 2 channel + 4 size + 1 frame-end),
// so body content per body frame is `frame_max - 8`.
test "frame_max: body sizes at and around the per-frame boundary roundtrip" {
    const _t = h.TestTimer.start("frame_max: body sizes at and around the per-frame boundary roundtrip");
    defer _t.stop();
    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .frame_max = 8192,
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = "bunny-zig.test.frame-max-boundary";
    _ = try ch.queueDeclare(q, .{ .exclusive = true, .auto_delete = true });

    const per_frame: usize = @as(usize, conn.negotiated_frame_max) - 8;
    const sizes = [_]usize{
        0,
        1,
        per_frame - 1,
        per_frame,
        per_frame + 1,
        2 * per_frame,
        2 * per_frame + 1,
    };

    var body = try h.test_allocator.alloc(u8, 2 * per_frame + 1);
    defer h.test_allocator.free(body);
    for (body, 0..) |*b, i| b.* = @intCast(i & 0xFF);

    try ch.confirmSelect();
    for (sizes) |size| {
        try ch.publishToQueue(q, body[0..size], .{});
        _ = try ch.waitForConfirms();

        const got = try h.pollBasicGet(ch, q);
        try testing.expect(got != null);
        var msg = got.?;
        defer msg.deinit(h.test_allocator);
        try testing.expectEqual(size, msg.body.len);
        try testing.expectEqualSlices(u8, body[0..size], msg.body);
        try ch.basicAck(msg.delivery_tag, false);
    }
}
