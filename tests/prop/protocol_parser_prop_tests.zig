//! Property-based tests for parsers above the wire layer:
//! `BasicProperties`, `FieldTable`, `FieldValue`, and a representative
//! slice of `Method` variants.
const std = @import("std");
const pt = @import("proptest");
const bunny = @import("bunny");

const protocol = bunny.protocol;
const wire = protocol.wire;
const constants = protocol.constants;

const WireBuffer = wire.WireBuffer;
const WireReader = wire.WireReader;
const BasicProperties = protocol.BasicProperties;
const FieldTable = protocol.FieldTable;
const FieldValue = protocol.FieldValue;
const Method = protocol.Method;

const allocator = std.testing.allocator;

const ct_value: []const u8 = "application/json";
const ce_value: []const u8 = "utf-8";
const cid_value: []const u8 = "cid";
const rt_value: []const u8 = "rt";
const exp_value: []const u8 = "30000";
const mid_value: []const u8 = "mid";
const type_value: []const u8 = "evt";
const uid_value: []const u8 = "guest";
const aid_value: []const u8 = "app";
const cluster_value: []const u8 = "cluster";

fn basicPropertiesRoundtrip(p: struct { u16, u8, u8, u64 }) !void {
    const flags = p[0];
    const priority = p[1];
    const delivery_mode = p[2];
    const timestamp = p[3];

    var props = BasicProperties{};
    if (flags & constants.prop_content_type != 0) props.content_type = ct_value;
    if (flags & constants.prop_content_encoding != 0) props.content_encoding = ce_value;
    if (flags & constants.prop_delivery_mode != 0) props.delivery_mode = delivery_mode;
    if (flags & constants.prop_priority != 0) props.priority = priority;
    if (flags & constants.prop_correlation_id != 0) props.correlation_id = cid_value;
    if (flags & constants.prop_reply_to != 0) props.reply_to = rt_value;
    if (flags & constants.prop_expiration != 0) props.expiration = exp_value;
    if (flags & constants.prop_message_id != 0) props.message_id = mid_value;
    if (flags & constants.prop_timestamp != 0) props.timestamp = timestamp;
    if (flags & constants.prop_type != 0) props.type = type_value;
    if (flags & constants.prop_user_id != 0) props.user_id = uid_value;
    if (flags & constants.prop_app_id != 0) props.app_id = aid_value;
    if (flags & constants.prop_cluster_id != 0) props.cluster_id = cluster_value;

    var buf: [1024]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    props.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const decoded = try BasicProperties.decode(&reader, allocator);

    if (decoded.flags() != props.flags()) return error.FlagsMismatch;

    if (props.content_type) |v|
        if (!std.mem.eql(u8, decoded.content_type.?, v)) return error.ContentTypeMismatch;
    if (props.content_encoding) |v|
        if (!std.mem.eql(u8, decoded.content_encoding.?, v)) return error.ContentEncodingMismatch;
    if (props.delivery_mode) |v|
        if (decoded.delivery_mode.? != v) return error.DeliveryModeMismatch;
    if (props.priority) |v|
        if (decoded.priority.? != v) return error.PriorityMismatch;
    if (props.correlation_id) |v|
        if (!std.mem.eql(u8, decoded.correlation_id.?, v)) return error.CorrelationIdMismatch;
    if (props.reply_to) |v|
        if (!std.mem.eql(u8, decoded.reply_to.?, v)) return error.ReplyToMismatch;
    if (props.expiration) |v|
        if (!std.mem.eql(u8, decoded.expiration.?, v)) return error.ExpirationMismatch;
    if (props.message_id) |v|
        if (!std.mem.eql(u8, decoded.message_id.?, v)) return error.MessageIdMismatch;
    if (props.timestamp) |v|
        if (decoded.timestamp.? != v) return error.TimestampMismatch;
    if (props.type) |v|
        if (!std.mem.eql(u8, decoded.type.?, v)) return error.TypeMismatch;
    if (props.user_id) |v|
        if (!std.mem.eql(u8, decoded.user_id.?, v)) return error.UserIdMismatch;
    if (props.app_id) |v|
        if (!std.mem.eql(u8, decoded.app_id.?, v)) return error.AppIdMismatch;
    if (props.cluster_id) |v|
        if (!std.mem.eql(u8, decoded.cluster_id.?, v)) return error.ClusterIdMismatch;
}

fn fieldValueI8Roundtrip(v: i8) !void {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    (FieldValue{ .i8 = v }).encode(&wb);
    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    if (result.value.i8 != v) return error.ValueMismatch;
    if (result.consumed != wb.getWritten().len) return error.ConsumedMismatch;
}

fn fieldValueU16Roundtrip(v: u16) !void {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    (FieldValue{ .u16 = v }).encode(&wb);
    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    if (result.value.u16 != v) return error.ValueMismatch;
    if (result.consumed != wb.getWritten().len) return error.ConsumedMismatch;
}

fn fieldValueI32Roundtrip(v: i32) !void {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    (FieldValue{ .i32 = v }).encode(&wb);
    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    if (result.value.i32 != v) return error.ValueMismatch;
    if (result.consumed != wb.getWritten().len) return error.ConsumedMismatch;
}

fn fieldValueI64Roundtrip(v: i64) !void {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    (FieldValue{ .i64 = v }).encode(&wb);
    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    if (result.value.i64 != v) return error.ValueMismatch;
    if (result.consumed != wb.getWritten().len) return error.ConsumedMismatch;
}

fn fieldValueTimestampRoundtrip(v: u64) !void {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    (FieldValue{ .timestamp = v }).encode(&wb);
    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    if (result.value.timestamp != v) return error.ValueMismatch;
    if (result.consumed != wb.getWritten().len) return error.ConsumedMismatch;
}

fn fieldValueDecimalRoundtrip(p: struct { u8, u32 }) !void {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    (FieldValue{ .decimal = .{ .scale = p[0], .value = p[1] } }).encode(&wb);
    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    if (result.value.decimal.scale != p[0]) return error.ScaleMismatch;
    if (result.value.decimal.value != p[1]) return error.ValueMismatch;
}

fn fieldValueLongStringRoundtrip(s: []const u8) !void {
    var buf: [4200]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    (FieldValue{ .long_string = s }).encode(&wb);
    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    if (!std.mem.eql(u8, result.value.long_string, s)) return error.StringMismatch;
}

fn fieldValueByteArrayRoundtrip(s: []const u8) !void {
    var buf: [4200]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    (FieldValue{ .byte_array = s }).encode(&wb);
    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    if (!std.mem.eql(u8, result.value.byte_array, s)) return error.ContentMismatch;
}

// Each seed encodes both the value-tag (low 3 bits) and the payload (the
// rest), so a single u32 generator covers the full FieldTable scalar
// space without needing a heterogeneous oneOf.
fn fieldTableRoundtrip(seeds: []const u32) !void {
    if (seeds.len > 32) return;

    var entries: std.ArrayList(FieldTable.Entry) = .empty;
    defer entries.deinit(allocator);

    var key_storage: [32][8]u8 = undefined;

    for (seeds, 0..) |s, i| {
        const tag = s & 0x7;
        const payload = s >> 3;
        const key = std.fmt.bufPrint(&key_storage[i], "k{d}", .{i}) catch unreachable;

        const value: FieldValue = switch (tag) {
            0 => .{ .boolean = (payload & 1) != 0 },
            1 => .{ .i32 = @bitCast(payload) },
            2 => .{ .u32 = payload },
            3 => .{ .i64 = @as(i64, @bitCast(@as(u64, payload))) },
            4 => .{ .timestamp = payload },
            5 => .{ .void = {} },
            6 => .{ .u8 = @truncate(payload) },
            7 => .{ .i16 = @bitCast(@as(u16, @truncate(payload))) },
            else => unreachable,
        };
        try entries.append(allocator, .{ .key = key, .value = value });
    }

    const owned = try allocator.dupe(FieldTable.Entry, entries.items);
    var table: FieldTable = .{ .entries = owned, .allocator = allocator };
    defer table.deinit();

    var buf: [4096]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    table.encode(&wb);

    const result = try FieldTable.decode(wb.getWritten(), allocator);
    var decoded = result.table;
    defer decoded.deinit();

    if (decoded.entries.len != entries.items.len) return error.LengthMismatch;
    if (result.consumed != wb.getWritten().len) return error.ConsumedMismatch;

    for (entries.items, decoded.entries) |orig, dec| {
        if (!std.mem.eql(u8, orig.key, dec.key)) return error.KeyMismatch;
        switch (orig.value) {
            .boolean => |v| if (dec.value.boolean != v) return error.BoolMismatch,
            .i32 => |v| if (dec.value.i32 != v) return error.I32Mismatch,
            .u32 => |v| if (dec.value.u32 != v) return error.U32Mismatch,
            .i64 => |v| if (dec.value.i64 != v) return error.I64Mismatch,
            .timestamp => |v| if (dec.value.timestamp != v) return error.TimestampMismatch,
            .void => if (dec.value != .void) return error.VoidMismatch,
            .u8 => |v| if (dec.value.u8 != v) return error.U8Mismatch,
            .i16 => |v| if (dec.value.i16 != v) return error.I16Mismatch,
            else => unreachable,
        }
    }
}

fn exchangeDeclareRoundtrip(p: struct { []const u8, []const u8, u8 }) !void {
    const ex = p[0];
    const ex_type = p[1];
    const bits = p[2];

    const m = Method{ .exchange_declare = .{
        .exchange = ex,
        .exchange_type = ex_type,
        .passive = bits & 1 != 0,
        .durable = bits & 2 != 0,
        .auto_delete = bits & 4 != 0,
        .internal = bits & 8 != 0,
        .no_wait = bits & 16 != 0,
    } };

    var buf: [1024]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    m.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const class_id = try reader.readU16();
    const method_id = try reader.readU16();
    var decoded = try Method.decode(class_id, method_id, &reader, allocator);
    defer switch (decoded) {
        .exchange_declare => |*ed| ed.arguments.deinit(),
        else => {},
    };

    const ed = decoded.exchange_declare;
    if (!std.mem.eql(u8, ed.exchange, ex)) return error.ExchangeMismatch;
    if (!std.mem.eql(u8, ed.exchange_type, ex_type)) return error.TypeMismatch;
    if (ed.passive != (bits & 1 != 0)) return error.PassiveMismatch;
    if (ed.durable != (bits & 2 != 0)) return error.DurableMismatch;
    if (ed.auto_delete != (bits & 4 != 0)) return error.AutoDeleteMismatch;
    if (ed.internal != (bits & 8 != 0)) return error.InternalMismatch;
    if (ed.no_wait != (bits & 16 != 0)) return error.NoWaitMismatch;
}

fn basicAckRoundtrip(p: struct { u64, u8 }) !void {
    const m = Method{ .basic_ack = .{
        .delivery_tag = p[0],
        .multiple = (p[1] & 1) != 0,
    } };

    var buf: [64]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    m.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const class_id = try reader.readU16();
    const method_id = try reader.readU16();
    const decoded = try Method.decode(class_id, method_id, &reader, allocator);

    const ack = decoded.basic_ack;
    if (ack.delivery_tag != p[0]) return error.TagMismatch;
    if (ack.multiple != ((p[1] & 1) != 0)) return error.MultipleMismatch;
}

fn basicNackRoundtrip(p: struct { u64, u8 }) !void {
    const m = Method{ .basic_nack = .{
        .delivery_tag = p[0],
        .multiple = (p[1] & 1) != 0,
        .requeue = (p[1] & 2) != 0,
    } };

    var buf: [64]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    m.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const class_id = try reader.readU16();
    const method_id = try reader.readU16();
    const decoded = try Method.decode(class_id, method_id, &reader, allocator);

    const nack = decoded.basic_nack;
    if (nack.delivery_tag != p[0]) return error.TagMismatch;
    if (nack.multiple != ((p[1] & 1) != 0)) return error.MultipleMismatch;
    if (nack.requeue != ((p[1] & 2) != 0)) return error.RequeueMismatch;
}

test "parser prop: BasicProperties standalone roundtrip across flag subsets" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    // FieldTable headers are exercised separately; mask that bit out here.
    const exclude_headers: u16 = ~constants.prop_headers;
    const flags_strat = pt.map(pt.num.int(u16), u16, struct {
        fn f(x: u16) u16 {
            return x & exclude_headers;
        }
    }.f);
    const strat = pt.tuple.t4(flags_strat, pt.num.int(u8), pt.num.int(u8), pt.num.int(u64));
    try runner.check(allocator, strat, basicPropertiesRoundtrip);
}

test "parser prop: FieldValue.i8 roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(i8), fieldValueI8Roundtrip);
}

test "parser prop: FieldValue.u16 roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(u16), fieldValueU16Roundtrip);
}

test "parser prop: FieldValue.i32 roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(i32), fieldValueI32Roundtrip);
}

test "parser prop: FieldValue.i64 roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(i64), fieldValueI64Roundtrip);
}

test "parser prop: FieldValue.timestamp roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(u64), fieldValueTimestampRoundtrip);
}

test "parser prop: FieldValue.decimal roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.tuple.t2(pt.num.int(u8), pt.num.int(u32));
    try runner.check(allocator, strat, fieldValueDecimalRoundtrip);
}

test "parser prop: FieldValue.long_string roundtrip with binary content" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.bytes(0, 1024), fieldValueLongStringRoundtrip);
}

test "parser prop: FieldValue.byte_array roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.bytes(0, 1024), fieldValueByteArrayRoundtrip);
}

test "parser prop: FieldTable with mixed scalar entries roundtrips" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.collection.slice(pt.num.int(u32), 0, 32);
    try runner.check(allocator, strat, fieldTableRoundtrip);
}

test "parser prop: exchange.declare method roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.tuple.t3(
        pt.collection.asciiString(0, 64),
        pt.collection.asciiString(0, 32),
        pt.num.int(u8),
    );
    try runner.check(allocator, strat, exchangeDeclareRoundtrip);
}

test "parser prop: basic.ack method roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.tuple.t2(pt.num.int(u64), pt.num.int(u8));
    try runner.check(allocator, strat, basicAckRoundtrip);
}

test "parser prop: basic.nack method roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.tuple.t2(pt.num.int(u64), pt.num.int(u8));
    try runner.check(allocator, strat, basicNackRoundtrip);
}
