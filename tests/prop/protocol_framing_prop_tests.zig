//! Property-based tests for `src/protocol/frame.zig`.
const std = @import("std");
const pt = @import("proptest");
const bunny = @import("bunny");

const protocol = bunny.protocol;
const frame_mod = protocol.frame;
const Frame = protocol.Frame;
const constants = protocol.constants;
const BasicProperties = protocol.BasicProperties;
const Method = protocol.Method;

const allocator = std.testing.allocator;

// Canonical string values for each present-by-flag property field.
const ct_value: []const u8 = "text/plain";
const ce_value: []const u8 = "utf-8";
const cid_value: []const u8 = "corr-id";
const rt_value: []const u8 = "reply.queue";
const exp_value: []const u8 = "60000";
const mid_value: []const u8 = "msg-id";
const type_value: []const u8 = "order.created";
const uid_value: []const u8 = "guest";
const aid_value: []const u8 = "test-app";
const cluster_value: []const u8 = "cluster";

fn bodyFrameRoundtrip(p: struct { u16, []const u8 }) !void {
    const ch = p[0];
    const payload = p[1];
    var buf: [2048]u8 = undefined;
    const encoded = frame_mod.encodeFrame(&buf, .{ .body = .{
        .channel = ch,
        .payload = payload,
    } });
    if (encoded.len != constants.frame_overhead + payload.len) return error.WrongLength;
    if (encoded[encoded.len - 1] != constants.frame_end) return error.WrongTerminator;

    const result = (try frame_mod.decodeFrame(encoded, allocator)) orelse
        return error.DecodeReturnedNull;
    if (result.consumed != encoded.len) return error.PartialConsume;
    if (result.frame.body.channel != ch) return error.ChannelMismatch;
    if (!std.mem.eql(u8, result.frame.body.payload, payload)) return error.PayloadMismatch;
    if (result.frame.channel() != ch) return error.AccessorMismatch;
}

fn headerFrameRoundtrip(p: struct { u16, u16, u64, u8, u8 }) !void {
    const ch = p[0];
    const flags = p[1];
    const body_size = p[2];
    const priority = p[3];
    const delivery_mode = p[4];

    var props = BasicProperties{};
    if (flags & constants.prop_content_type != 0) props.content_type = ct_value;
    if (flags & constants.prop_content_encoding != 0) props.content_encoding = ce_value;
    if (flags & constants.prop_delivery_mode != 0) props.delivery_mode = delivery_mode;
    if (flags & constants.prop_priority != 0) props.priority = priority;
    if (flags & constants.prop_correlation_id != 0) props.correlation_id = cid_value;
    if (flags & constants.prop_reply_to != 0) props.reply_to = rt_value;
    if (flags & constants.prop_expiration != 0) props.expiration = exp_value;
    if (flags & constants.prop_message_id != 0) props.message_id = mid_value;
    if (flags & constants.prop_timestamp != 0) props.timestamp = 1_700_000_000;
    if (flags & constants.prop_type != 0) props.type = type_value;
    if (flags & constants.prop_user_id != 0) props.user_id = uid_value;
    if (flags & constants.prop_app_id != 0) props.app_id = aid_value;
    if (flags & constants.prop_cluster_id != 0) props.cluster_id = cluster_value;

    var buf: [1024]u8 = undefined;
    const encoded = frame_mod.encodeFrame(&buf, .{ .header = .{
        .channel = ch,
        .class_id = constants.class_basic,
        .body_size = body_size,
        .properties = props,
    } });
    if (encoded[encoded.len - 1] != constants.frame_end) return error.WrongTerminator;

    const result = (try frame_mod.decodeFrame(encoded, allocator)) orelse
        return error.DecodeReturnedNull;
    if (result.consumed != encoded.len) return error.PartialConsume;
    const h = result.frame.header;
    if (h.channel != ch) return error.ChannelMismatch;
    if (h.class_id != constants.class_basic) return error.ClassIdMismatch;
    if (h.body_size != body_size) return error.BodySizeMismatch;
    if (h.properties.flags() != props.flags()) return error.FlagsMismatch;

    if (props.content_type) |v|
        if (!std.mem.eql(u8, h.properties.content_type.?, v)) return error.ContentTypeMismatch;
    if (props.content_encoding) |v|
        if (!std.mem.eql(u8, h.properties.content_encoding.?, v)) return error.ContentEncodingMismatch;
    if (props.delivery_mode) |v|
        if (h.properties.delivery_mode.? != v) return error.DeliveryModeMismatch;
    if (props.priority) |v|
        if (h.properties.priority.? != v) return error.PriorityMismatch;
    if (props.correlation_id) |v|
        if (!std.mem.eql(u8, h.properties.correlation_id.?, v)) return error.CorrelationIdMismatch;
    if (props.reply_to) |v|
        if (!std.mem.eql(u8, h.properties.reply_to.?, v)) return error.ReplyToMismatch;
    if (props.expiration) |v|
        if (!std.mem.eql(u8, h.properties.expiration.?, v)) return error.ExpirationMismatch;
    if (props.message_id) |v|
        if (!std.mem.eql(u8, h.properties.message_id.?, v)) return error.MessageIdMismatch;
    if (props.timestamp) |v|
        if (h.properties.timestamp.? != v) return error.TimestampMismatch;
    if (props.type) |v|
        if (!std.mem.eql(u8, h.properties.type.?, v)) return error.TypeMismatch;
    if (props.user_id) |v|
        if (!std.mem.eql(u8, h.properties.user_id.?, v)) return error.UserIdMismatch;
    if (props.app_id) |v|
        if (!std.mem.eql(u8, h.properties.app_id.?, v)) return error.AppIdMismatch;
    if (props.cluster_id) |v|
        if (!std.mem.eql(u8, h.properties.cluster_id.?, v)) return error.ClusterIdMismatch;
}

fn basicPublishRoundtrip(p: struct { u16, []const u8, []const u8, u8 }) !void {
    const ch = p[0];
    const exchange = p[1];
    const routing_key = p[2];
    const flags_byte = p[3];
    const m = Method{ .basic_publish = .{
        .exchange = exchange,
        .routing_key = routing_key,
        .mandatory = flags_byte & 1 != 0,
        .immediate = flags_byte & 2 != 0,
    } };

    var buf: [1024]u8 = undefined;
    const encoded = frame_mod.encodeFrame(&buf, .{ .method = .{ .channel = ch, .method = m } });

    const result = (try frame_mod.decodeFrame(encoded, allocator)) orelse
        return error.DecodeReturnedNull;
    if (result.consumed != encoded.len) return error.PartialConsume;

    const decoded = result.frame.method;
    if (decoded.channel != ch) return error.ChannelMismatch;
    const bp = decoded.method.basic_publish;
    if (!std.mem.eql(u8, bp.exchange, exchange)) return error.ExchangeMismatch;
    if (!std.mem.eql(u8, bp.routing_key, routing_key)) return error.RoutingKeyMismatch;
    if (bp.mandatory != (flags_byte & 1 != 0)) return error.MandatoryMismatch;
    if (bp.immediate != (flags_byte & 2 != 0)) return error.ImmediateMismatch;
}

fn truncatedDecodeReturnsNull(p: struct { u16, []const u8, u32 }) !void {
    const ch = p[0];
    const payload = p[1];
    const cut_seed = p[2];

    var buf: [4096]u8 = undefined;
    const encoded = frame_mod.encodeFrame(&buf, .{ .body = .{
        .channel = ch,
        .payload = payload,
    } });
    // Cut anywhere strictly before the final frame_end byte; past that the
    // frame is complete and the decoder must succeed.
    const cut = (cut_seed % @as(u32, @intCast(encoded.len - 1))) + 1;
    const result = try frame_mod.decodeFrame(encoded[0..cut], allocator);
    if (result != null) return error.ShouldHaveReturnedNull;
}

fn invalidFrameEndIsRejected(p: struct { u16, []const u8, u8 }) !void {
    const ch = p[0];
    const payload = p[1];
    const bad_end = p[2];
    if (bad_end == constants.frame_end) return; // accidentally valid

    var buf: [4096]u8 = undefined;
    const encoded_len = frame_mod.encodeFrame(&buf, .{ .body = .{
        .channel = ch,
        .payload = payload,
    } }).len;
    buf[encoded_len - 1] = bad_end;

    const result = frame_mod.decodeFrame(buf[0..encoded_len], allocator);
    if (result) |_| return error.ShouldHaveErrored else |err| {
        if (err != error.InvalidFrameEnd) return error.WrongError;
    }
}

test "framing prop: body frame roundtrip across channel ids and payloads" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.tuple.t2(pt.num.int(u16), pt.collection.bytes(0, 1024));
    try runner.check(allocator, strat, bodyFrameRoundtrip);
}

test "framing prop: header frame roundtrip across BasicProperties subsets" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    // FieldTable headers are exercised separately; mask that bit out here.
    const exclude_headers: u16 = ~constants.prop_headers;
    const flags_strat = pt.map(pt.num.int(u16), u16, struct {
        fn f(x: u16) u16 {
            return x & exclude_headers;
        }
    }.f);
    // t4 is the largest tuple combinator; pack two u8s into a single u16
    // so the predicate sees five logical fields.
    const Mapper = struct {
        fn pack(v: struct { u16, u16, u64, u16 }) struct { u16, u16, u64, u8, u8 } {
            const both = v[3];
            return .{ v[0], v[1], v[2], @truncate(both), @truncate(both >> 8) };
        }
    };
    const strat = pt.map(pt.tuple.t4(
        pt.num.int(u16),
        flags_strat,
        pt.num.int(u64),
        pt.num.int(u16),
    ), struct { u16, u16, u64, u8, u8 }, Mapper.pack);
    try runner.check(allocator, strat, headerFrameRoundtrip);
}

test "framing prop: basic.publish method frame roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.tuple.t4(
        pt.num.int(u16),
        pt.collection.asciiString(0, 64),
        pt.collection.asciiString(0, 64),
        pt.num.int(u8),
    );
    try runner.check(allocator, strat, basicPublishRoundtrip);
}

test "framing prop: truncated frames decode to null without error" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.tuple.t3(
        pt.num.int(u16),
        pt.collection.bytes(0, 256),
        pt.num.int(u32),
    );
    try runner.check(allocator, strat, truncatedDecodeReturnsNull);
}

test "framing prop: any non-0xCE terminator is rejected" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.tuple.t3(
        pt.num.int(u16),
        pt.collection.bytes(0, 64),
        pt.num.int(u8),
    );
    try runner.check(allocator, strat, invalidFrameEndIsRejected);
}

test "framing prop: Frame.channel() reflects the inner frame" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(u16), struct {
        fn f(ch: u16) !void {
            const hb = Frame{ .heartbeat = {} };
            if (hb.channel() != 0) return error.HeartbeatChannelNotZero;
            const body = Frame{ .body = .{ .channel = ch, .payload = "" } };
            if (body.channel() != ch) return error.BodyChannelMismatch;
        }
    }.f);
}
