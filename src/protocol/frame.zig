/// AMQP 0-9-1 frame encoding and decoding.
const std = @import("std");
const Allocator = std.mem.Allocator;
const c = @import("constants.zig");
const method_mod = @import("method.zig");
const Method = method_mod.Method;
const properties = @import("properties.zig");
const BasicProperties = properties.BasicProperties;
const WireBuffer = @import("wire.zig").WireBuffer;
const WireReader = @import("wire.zig").WireReader;
const types = @import("types.zig");

/// A decoded AMQP frame.
pub const Frame = union(FrameType) {
    method: MethodFrame,
    header: HeaderFrame,
    body: BodyFrame,
    heartbeat: void,

    pub const MethodFrame = struct {
        channel: u16,
        method: Method,
    };

    pub const HeaderFrame = struct {
        channel: u16,
        class_id: u16,
        body_size: u64,
        properties: BasicProperties,
    };

    pub const BodyFrame = struct {
        channel: u16,
        payload: []const u8,
    };

    pub fn channel(self: Frame) u16 {
        return switch (self) {
            .method => |f| f.channel,
            .header => |f| f.channel,
            .body => |f| f.channel,
            .heartbeat => 0,
        };
    }
};

pub const FrameType = enum(u8) {
    method = c.frame_method,
    header = c.frame_header,
    body = c.frame_body,
    heartbeat = c.frame_heartbeat,
};

/// Encode a frame into a buffer. Returns the slice of bytes written.
pub fn encodeFrame(buf: []u8, frame: Frame) []const u8 {
    var wb = WireBuffer.init(buf);

    switch (frame) {
        .method => |f| {
            wb.writeByte(c.frame_method);
            wb.writeU16(f.channel);
            const size_pos = wb.pos();
            wb.writeU32(0); // placeholder
            const payload_start = wb.pos();
            f.method.encode(&wb);
            const payload_end = wb.pos();
            wb.patchU32(size_pos, @intCast(payload_end - payload_start));
            wb.writeByte(c.frame_end);
        },
        .header => |f| {
            wb.writeByte(c.frame_header);
            wb.writeU16(f.channel);
            const size_pos = wb.pos();
            wb.writeU32(0); // placeholder
            const payload_start = wb.pos();
            wb.writeU16(f.class_id);
            wb.writeU16(0); // weight (always 0)
            wb.writeU64(f.body_size);
            f.properties.encode(&wb);
            const payload_end = wb.pos();
            wb.patchU32(size_pos, @intCast(payload_end - payload_start));
            wb.writeByte(c.frame_end);
        },
        .body => |f| {
            wb.writeByte(c.frame_body);
            wb.writeU16(f.channel);
            wb.writeU32(@intCast(f.payload.len));
            wb.writeBytes(f.payload);
            wb.writeByte(c.frame_end);
        },
        .heartbeat => {
            wb.writeByte(c.frame_heartbeat);
            wb.writeU16(0);
            wb.writeU32(0);
            wb.writeByte(c.frame_end);
        },
    }

    return wb.getWritten();
}

/// Decode a frame from raw bytes. Returns the frame and number of bytes consumed.
/// Returns null if there isn't enough data for a complete frame.
pub fn decodeFrame(data: []const u8, allocator: Allocator) !?struct { frame: Frame, consumed: usize } {
    if (data.len < c.frame_overhead) return null;

    const frame_type = data[0];
    const channel_id = types.readU16(data[1..]);
    const payload_size = types.readU32(data[3..]);
    const total_size = c.frame_overhead + payload_size;

    if (data.len < total_size) return null;

    const frame_end = data[total_size - 1];
    if (frame_end != c.frame_end) return error.InvalidFrameEnd;

    const payload = data[c.frame_header_size..][0..payload_size];

    const frame: Frame = switch (frame_type) {
        c.frame_method => blk: {
            var reader = WireReader.init(payload);
            const class_id = try reader.readU16();
            const method_id = try reader.readU16();
            const m = try Method.decode(class_id, method_id, &reader, allocator);
            break :blk .{ .method = .{ .channel = channel_id, .method = m } };
        },
        c.frame_header => blk: {
            var reader = WireReader.init(payload);
            const class_id = try reader.readU16();
            _ = try reader.readU16(); // weight
            const body_size = try reader.readU64();
            const props = try BasicProperties.decode(&reader, allocator);
            break :blk .{ .header = .{
                .channel = channel_id,
                .class_id = class_id,
                .body_size = body_size,
                .properties = props,
            } };
        },
        c.frame_body => .{ .body = .{
            .channel = channel_id,
            .payload = payload,
        } },
        c.frame_heartbeat => .{ .heartbeat = {} },
        else => return error.UnknownFrameType,
    };

    return .{ .frame = frame, .consumed = total_size };
}

// Tests
test "heartbeat frame roundtrip" {
    var buf: [64]u8 = undefined;
    const encoded = encodeFrame(&buf, .{ .heartbeat = {} });
    try std.testing.expectEqual(@as(usize, 8), encoded.len);

    const result = (try decodeFrame(encoded, std.testing.allocator)).?;
    try std.testing.expect(result.frame == .heartbeat);
    try std.testing.expectEqual(@as(usize, 8), result.consumed);
}

test "method frame roundtrip" {
    var buf: [512]u8 = undefined;
    const m = Method{ .basic_publish = .{
        .exchange = "test",
        .routing_key = "key",
        .mandatory = true,
    } };
    const encoded = encodeFrame(&buf, .{ .method = .{ .channel = 1, .method = m } });
    const result = (try decodeFrame(encoded, std.testing.allocator)).?;

    try std.testing.expectEqual(@as(u16, 1), result.frame.method.channel);
    try std.testing.expectEqualSlices(u8, "test", result.frame.method.method.basic_publish.exchange);
    try std.testing.expect(result.frame.method.method.basic_publish.mandatory);
}

test "header frame roundtrip" {
    var buf: [512]u8 = undefined;
    const props = BasicProperties{
        .content_type = "text/plain",
        .delivery_mode = 2,
    };
    const encoded = encodeFrame(&buf, .{ .header = .{
        .channel = 1,
        .class_id = c.class_basic,
        .body_size = 100,
        .properties = props,
    } });
    const result = (try decodeFrame(encoded, std.testing.allocator)).?;

    try std.testing.expectEqual(@as(u16, 1), result.frame.header.channel);
    try std.testing.expectEqual(@as(u64, 100), result.frame.header.body_size);
    try std.testing.expectEqualSlices(u8, "text/plain", result.frame.header.properties.content_type.?);
    try std.testing.expectEqual(@as(u8, 2), result.frame.header.properties.delivery_mode.?);
}

test "body frame roundtrip" {
    var buf: [512]u8 = undefined;
    const payload = "Hello, AMQP!";
    const encoded = encodeFrame(&buf, .{ .body = .{
        .channel = 3,
        .payload = payload,
    } });
    const result = (try decodeFrame(encoded, std.testing.allocator)).?;

    try std.testing.expectEqual(@as(u16, 3), result.frame.body.channel);
    try std.testing.expectEqualSlices(u8, payload, result.frame.body.payload);
}

test "incomplete frame returns null" {
    const data = [_]u8{ c.frame_heartbeat, 0, 0 };
    const result = try decodeFrame(&data, std.testing.allocator);
    try std.testing.expect(result == null);
}

test "invalid frame end returns error" {
    const data = [_]u8{ c.frame_heartbeat, 0, 0, 0, 0, 0, 0, 0xFF };
    const result = decodeFrame(&data, std.testing.allocator);
    try std.testing.expectError(error.InvalidFrameEnd, result);
}

test "channel accessor" {
    const hb = Frame{ .heartbeat = {} };
    try std.testing.expectEqual(@as(u16, 0), hb.channel());

    const body = Frame{ .body = .{ .channel = 7, .payload = "x" } };
    try std.testing.expectEqual(@as(u16, 7), body.channel());
}

test "empty body frame roundtrip" {
    var buf: [64]u8 = undefined;
    const encoded = encodeFrame(&buf, .{ .body = .{ .channel = 1, .payload = "" } });
    const result = (try decodeFrame(encoded, std.testing.allocator)).?;
    try std.testing.expectEqual(@as(usize, 0), result.frame.body.payload.len);
}

test "method frame: exchange declare roundtrip" {
    const allocator = std.testing.allocator;
    var buf: [512]u8 = undefined;
    const m = Method{ .exchange_declare = .{
        .exchange = "logs",
        .exchange_type = "fanout",
        .durable = true,
        .auto_delete = false,
    } };
    const encoded = encodeFrame(&buf, .{ .method = .{ .channel = 2, .method = m } });
    const result = (try decodeFrame(encoded, allocator)).?;

    const ed = result.frame.method.method.exchange_declare;
    try std.testing.expectEqualSlices(u8, "logs", ed.exchange);
    try std.testing.expectEqualSlices(u8, "fanout", ed.exchange_type);
    try std.testing.expect(ed.durable);
    try std.testing.expect(!ed.auto_delete);
}

test "method frame: basic nack roundtrip" {
    var buf: [128]u8 = undefined;
    const m = Method{ .basic_nack = .{
        .delivery_tag = 42,
        .multiple = true,
        .requeue = false,
    } };
    const encoded = encodeFrame(&buf, .{ .method = .{ .channel = 1, .method = m } });
    const result = (try decodeFrame(encoded, std.testing.allocator)).?;

    const nack = result.frame.method.method.basic_nack;
    try std.testing.expectEqual(@as(u64, 42), nack.delivery_tag);
    try std.testing.expect(nack.multiple);
}

test "multiple frames decoded sequentially" {
    var buf: [1024]u8 = undefined;
    var offset: usize = 0;

    // Encode two frames back to back
    const f1 = encodeFrame(buf[offset..], .{ .heartbeat = {} });
    offset += f1.len;
    const f2 = encodeFrame(buf[offset..], .{ .body = .{ .channel = 1, .payload = "hello" } });
    offset += f2.len;

    const allocator = std.testing.allocator;
    var pos: usize = 0;

    const r1 = (try decodeFrame(buf[pos..offset], allocator)).?;
    try std.testing.expect(r1.frame == .heartbeat);
    pos += r1.consumed;

    const r2 = (try decodeFrame(buf[pos..offset], allocator)).?;
    try std.testing.expectEqualSlices(u8, "hello", r2.frame.body.payload);
}

test "fuzz: frame decode does not crash on arbitrary input" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *std.testing.Smith) !void {
            var buf: [512]u8 = undefined;
            const len = smith.sliceWithHash(&buf, 0);
            const allocator = std.testing.allocator;
            _ = decodeFrame(buf[0..len], allocator) catch return;
        }
    }.f, .{});
}
