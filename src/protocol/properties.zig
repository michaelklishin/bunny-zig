/// AMQP 0-9-1 Basic message properties with flag-based serialization.
const std = @import("std");
const Allocator = std.mem.Allocator;
const constants = @import("constants.zig");
const types = @import("types.zig");
const WireBuffer = @import("wire.zig").WireBuffer;
const WireReader = @import("wire.zig").WireReader;
const FieldTable = types.FieldTable;

pub const BasicProperties = struct {
    content_type: ?[]const u8 = null,
    content_encoding: ?[]const u8 = null,
    headers: ?FieldTable = null,
    delivery_mode: ?u8 = null,
    priority: ?u8 = null,
    correlation_id: ?[]const u8 = null,
    reply_to: ?[]const u8 = null,
    expiration: ?[]const u8 = null,
    message_id: ?[]const u8 = null,
    timestamp: ?u64 = null,
    type: ?[]const u8 = null,
    user_id: ?[]const u8 = null,
    app_id: ?[]const u8 = null,
    cluster_id: ?[]const u8 = null,

    pub const default: BasicProperties = .{};

    pub const persistent: BasicProperties = .{
        .delivery_mode = constants.delivery_mode_persistent,
    };

    pub const transient: BasicProperties = .{
        .delivery_mode = constants.delivery_mode_transient,
    };

    // Builder-style methods
    pub fn withContentType(self: BasicProperties, ct: []const u8) BasicProperties {
        var p = self;
        p.content_type = ct;
        return p;
    }

    pub fn withContentEncoding(self: BasicProperties, ce: []const u8) BasicProperties {
        var p = self;
        p.content_encoding = ce;
        return p;
    }

    pub fn withHeaders(self: BasicProperties, h: FieldTable) BasicProperties {
        var p = self;
        p.headers = h;
        return p;
    }

    pub fn withDeliveryMode(self: BasicProperties, dm: u8) BasicProperties {
        var p = self;
        p.delivery_mode = dm;
        return p;
    }

    pub fn asPersistent(self: BasicProperties) BasicProperties {
        return self.withDeliveryMode(constants.delivery_mode_persistent);
    }

    pub fn withPriority(self: BasicProperties, pri: u8) BasicProperties {
        var p = self;
        p.priority = pri;
        return p;
    }

    pub fn withCorrelationId(self: BasicProperties, cid: []const u8) BasicProperties {
        var p = self;
        p.correlation_id = cid;
        return p;
    }

    pub fn withReplyTo(self: BasicProperties, rt: []const u8) BasicProperties {
        var p = self;
        p.reply_to = rt;
        return p;
    }

    pub fn withExpiration(self: BasicProperties, exp: []const u8) BasicProperties {
        var p = self;
        p.expiration = exp;
        return p;
    }

    pub fn withMessageId(self: BasicProperties, mid: []const u8) BasicProperties {
        var p = self;
        p.message_id = mid;
        return p;
    }

    pub fn withTimestamp(self: BasicProperties, ts: u64) BasicProperties {
        var p = self;
        p.timestamp = ts;
        return p;
    }

    pub fn withType(self: BasicProperties, t: []const u8) BasicProperties {
        var p = self;
        p.type = t;
        return p;
    }

    pub fn withUserId(self: BasicProperties, uid: []const u8) BasicProperties {
        var p = self;
        p.user_id = uid;
        return p;
    }

    pub fn withAppId(self: BasicProperties, aid: []const u8) BasicProperties {
        var p = self;
        p.app_id = aid;
        return p;
    }

    /// Compute the flags bitmask.
    pub fn flags(self: BasicProperties) u16 {
        var f: u16 = 0;
        if (self.content_type != null) f |= constants.prop_content_type;
        if (self.content_encoding != null) f |= constants.prop_content_encoding;
        if (self.headers != null) f |= constants.prop_headers;
        if (self.delivery_mode != null) f |= constants.prop_delivery_mode;
        if (self.priority != null) f |= constants.prop_priority;
        if (self.correlation_id != null) f |= constants.prop_correlation_id;
        if (self.reply_to != null) f |= constants.prop_reply_to;
        if (self.expiration != null) f |= constants.prop_expiration;
        if (self.message_id != null) f |= constants.prop_message_id;
        if (self.timestamp != null) f |= constants.prop_timestamp;
        if (self.type != null) f |= constants.prop_type;
        if (self.user_id != null) f |= constants.prop_user_id;
        if (self.app_id != null) f |= constants.prop_app_id;
        if (self.cluster_id != null) f |= constants.prop_cluster_id;
        return f;
    }

    /// Encode properties to wire format (flags + present fields).
    pub fn encode(self: BasicProperties, wb: *WireBuffer) void {
        const f = self.flags();
        wb.writeU16(f);
        if (f == 0) return;

        if (self.content_type) |v| wb.writeShortString(v);
        if (self.content_encoding) |v| wb.writeShortString(v);
        if (self.headers) |h| h.encode(wb);
        if (self.delivery_mode) |v| wb.writeByte(v);
        if (self.priority) |v| wb.writeByte(v);
        if (self.correlation_id) |v| wb.writeShortString(v);
        if (self.reply_to) |v| wb.writeShortString(v);
        if (self.expiration) |v| wb.writeShortString(v);
        if (self.message_id) |v| wb.writeShortString(v);
        if (self.timestamp) |v| wb.writeU64(v);
        if (self.type) |v| wb.writeShortString(v);
        if (self.user_id) |v| wb.writeShortString(v);
        if (self.app_id) |v| wb.writeShortString(v);
        if (self.cluster_id) |v| wb.writeShortString(v);
    }

    /// Decode properties from wire format.
    pub fn decode(reader: *WireReader, allocator: Allocator) !BasicProperties {
        const f = try reader.readU16();
        var props = BasicProperties{};

        if (f & constants.prop_content_type != 0)
            props.content_type = try reader.readShortString();
        if (f & constants.prop_content_encoding != 0)
            props.content_encoding = try reader.readShortString();
        if (f & constants.prop_headers != 0) {
            const result = try FieldTable.decode(reader.rest(), allocator);
            props.headers = result.table;
            _ = try reader.readBytes(result.consumed);
        }
        if (f & constants.prop_delivery_mode != 0)
            props.delivery_mode = try reader.readByte();
        if (f & constants.prop_priority != 0)
            props.priority = try reader.readByte();
        if (f & constants.prop_correlation_id != 0)
            props.correlation_id = try reader.readShortString();
        if (f & constants.prop_reply_to != 0)
            props.reply_to = try reader.readShortString();
        if (f & constants.prop_expiration != 0)
            props.expiration = try reader.readShortString();
        if (f & constants.prop_message_id != 0)
            props.message_id = try reader.readShortString();
        if (f & constants.prop_timestamp != 0)
            props.timestamp = try reader.readU64();
        if (f & constants.prop_type != 0)
            props.type = try reader.readShortString();
        if (f & constants.prop_user_id != 0)
            props.user_id = try reader.readShortString();
        if (f & constants.prop_app_id != 0)
            props.app_id = try reader.readShortString();
        if (f & constants.prop_cluster_id != 0)
            props.cluster_id = try reader.readShortString();

        return props;
    }
};

// Tests
test "properties encode/decode roundtrip with all fields" {
    const allocator = std.testing.allocator;

    const original = BasicProperties{
        .content_type = "application/json",
        .content_encoding = "utf-8",
        .delivery_mode = constants.delivery_mode_persistent,
        .priority = 5,
        .correlation_id = "corr-123",
        .reply_to = "reply.queue",
        .expiration = "60000",
        .message_id = "msg-456",
        .timestamp = 1700000000,
        .type = "order.created",
        .user_id = "guest",
        .app_id = "test-app",
    };

    var buf: [1024]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    original.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const decoded = try BasicProperties.decode(&reader, allocator);

    try std.testing.expectEqualSlices(u8, "application/json", decoded.content_type.?);
    try std.testing.expectEqualSlices(u8, "utf-8", decoded.content_encoding.?);
    try std.testing.expectEqual(2, decoded.delivery_mode.?);
    try std.testing.expectEqual(5, decoded.priority.?);
    try std.testing.expectEqualSlices(u8, "corr-123", decoded.correlation_id.?);
    try std.testing.expectEqualSlices(u8, "reply.queue", decoded.reply_to.?);
    try std.testing.expectEqualSlices(u8, "60000", decoded.expiration.?);
    try std.testing.expectEqualSlices(u8, "msg-456", decoded.message_id.?);
    try std.testing.expectEqual(1700000000, decoded.timestamp.?);
    try std.testing.expectEqualSlices(u8, "order.created", decoded.type.?);
    try std.testing.expectEqualSlices(u8, "guest", decoded.user_id.?);
    try std.testing.expectEqualSlices(u8, "test-app", decoded.app_id.?);
}

test "properties encode/decode empty" {
    const allocator = std.testing.allocator;

    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    BasicProperties.default.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const decoded = try BasicProperties.decode(&reader, allocator);

    try std.testing.expectEqual(0, decoded.flags());
}

test "persistent preset" {
    const p = BasicProperties.persistent;
    try std.testing.expectEqual(2, p.delivery_mode.?);
}

test "builder pattern" {
    const p = BasicProperties.default
        .withContentType("text/plain")
        .asPersistent()
        .withMessageId("id-1");

    try std.testing.expectEqualSlices(u8, "text/plain", p.content_type.?);
    try std.testing.expectEqual(2, p.delivery_mode.?);
    try std.testing.expectEqualSlices(u8, "id-1", p.message_id.?);
    try std.testing.expect(p.priority == null);
}

test "properties: single field roundtrips" {
    const allocator = std.testing.allocator;

    // Test each field in isolation to verify flag bits
    const cases = [_]BasicProperties{
        BasicProperties.default.withContentType("text/plain"),
        BasicProperties.default.withContentEncoding("gzip"),
        BasicProperties.default.withDeliveryMode(1),
        BasicProperties.default.withPriority(9),
        BasicProperties.default.withCorrelationId("c-1"),
        BasicProperties.default.withReplyTo("reply.q"),
        BasicProperties.default.withExpiration("30000"),
        BasicProperties.default.withMessageId("m-1"),
        BasicProperties.default.withTimestamp(1700000000),
        BasicProperties.default.withType("order.created"),
        BasicProperties.default.withUserId("guest"),
        BasicProperties.default.withAppId("myapp"),
    };

    for (cases) |original| {
        var buf: [512]u8 = undefined;
        var wb = WireBuffer.init(&buf);
        original.encode(&wb);

        var reader = WireReader.init(wb.getWritten());
        const decoded = try BasicProperties.decode(&reader, allocator);

        try std.testing.expectEqual(original.flags(), decoded.flags());
    }
}

test "properties: flags bitmask correctness" {
    try std.testing.expectEqual(0, BasicProperties.default.flags());
    try std.testing.expectEqual(constants.prop_delivery_mode, BasicProperties.persistent.flags());

    const all = BasicProperties{
        .content_type = "a",
        .content_encoding = "b",
        .headers = types.FieldTable.init(std.testing.allocator),
        .delivery_mode = 2,
        .priority = 0,
        .correlation_id = "c",
        .reply_to = "d",
        .expiration = "e",
        .message_id = "f",
        .timestamp = 1,
        .type = "g",
        .user_id = "h",
        .app_id = "i",
        .cluster_id = "j",
    };
    try std.testing.expectEqual(0xFFFC, all.flags());
}

test "properties: with headers roundtrip" {
    const allocator = std.testing.allocator;
    const header_entries = [_]types.FieldTable.Entry{
        .{ .key = "x-retry", .value = .{ .i32 = 3 } },
    };
    var headers = try types.FieldTable.fromEntries(allocator, &header_entries);
    defer headers.deinit();

    const original = BasicProperties.default.withHeaders(headers).asPersistent();

    var buf: [512]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    original.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const decoded = try BasicProperties.decode(&reader, allocator);
    var decoded_headers = decoded.headers.?;
    defer decoded_headers.deinit();

    try std.testing.expectEqual(3, decoded_headers.get("x-retry").?.i32);
    try std.testing.expectEqual(2, decoded.delivery_mode.?);
}

test "fuzz: properties decode does not crash on arbitrary input" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *std.testing.Smith) !void {
            var buf: [256]u8 = undefined;
            const len = smith.sliceWithHash(&buf, 0);
            const allocator = std.testing.allocator;
            var reader = WireReader.init(buf[0..len]);
            const result = BasicProperties.decode(&reader, allocator) catch return;
            if (result.headers) |*h| {
                var headers = h.*;
                headers.deinit();
            }
        }
    }.f, .{});
}
