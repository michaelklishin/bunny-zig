/// AMQP 0-9-1 field tables and field values.
const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const WireBuffer = @import("wire.zig").WireBuffer;

pub const max_table_nesting: u8 = 16;

/// An AMQP field value with type tag.
pub const FieldValue = union(enum) {
    boolean: bool,
    i8: i8,
    u8: u8,
    i16: i16,
    u16: u16,
    i32: i32,
    u32: u32,
    i64: i64,
    f32: f32,
    f64: f64,
    decimal: Decimal,
    short_string: []const u8,
    long_string: []const u8,
    timestamp: u64,
    table: FieldTable,
    array: []const FieldValue,
    byte_array: []const u8,
    void: void,

    pub const Decimal = struct {
        scale: u8,
        value: u32,
    };

    /// Encode a field value to the wire format.
    pub fn encode(self: FieldValue, wb: *WireBuffer) void {
        switch (self) {
            .boolean => |v| {
                wb.writeByte('t');
                wb.writeByte(if (v) 1 else 0);
            },
            .i8 => |v| {
                wb.writeByte('b');
                wb.writeI8(v);
            },
            .u8 => |v| {
                wb.writeByte('B');
                wb.writeByte(v);
            },
            .i16 => |v| {
                wb.writeByte('s');
                wb.writeI16(v);
            },
            .u16 => |v| {
                wb.writeByte('u');
                wb.writeU16(v);
            },
            .i32 => |v| {
                wb.writeByte('I');
                wb.writeI32(v);
            },
            .u32 => |v| {
                wb.writeByte('i');
                wb.writeU32(v);
            },
            .i64 => |v| {
                wb.writeByte('l');
                wb.writeI64(v);
            },
            .f32 => |v| {
                wb.writeByte('f');
                wb.writeF32(v);
            },
            .f64 => |v| {
                wb.writeByte('d');
                wb.writeF64(v);
            },
            .decimal => |v| {
                wb.writeByte('D');
                wb.writeByte(v.scale);
                wb.writeU32(v.value);
            },
            .short_string => |v| {
                wb.writeByte('S');
                wb.writeLongString(v);
            },
            .long_string => |v| {
                wb.writeByte('S');
                wb.writeLongString(v);
            },
            .timestamp => |v| {
                wb.writeByte('T');
                wb.writeU64(v);
            },
            .table => |v| {
                wb.writeByte('F');
                v.encode(wb);
            },
            .array => |items| {
                wb.writeByte('A');
                const len_pos = wb.pos();
                wb.writeU32(0); // placeholder
                const start = wb.pos();
                for (items) |item| {
                    item.encode(wb);
                }
                const end = wb.pos();
                wb.patchU32(len_pos, @intCast(end - start));
            },
            .byte_array => |v| {
                wb.writeByte('x');
                wb.writeU32(@intCast(v.len));
                wb.writeBytes(v);
            },
            .void => {
                wb.writeByte('V');
            },
        }
    }

    /// Decode a field value from raw bytes.
    pub fn decode(data: []const u8, allocator: Allocator, depth: u8) DecodeError!DecodeResult {
        if (depth > max_table_nesting) return error.TableNestingTooDeep;
        if (data.len < 1) return error.InsufficientData;

        const tag = data[0];
        const rest = data[1..];

        switch (tag) {
            't' => {
                if (rest.len < 1) return error.InsufficientData;
                return .{ .value = .{ .boolean = rest[0] != 0 }, .consumed = 2 };
            },
            'b' => {
                if (rest.len < 1) return error.InsufficientData;
                return .{ .value = .{ .i8 = @bitCast(rest[0]) }, .consumed = 2 };
            },
            'B' => {
                if (rest.len < 1) return error.InsufficientData;
                return .{ .value = .{ .u8 = rest[0] }, .consumed = 2 };
            },
            's' => {
                if (rest.len < 2) return error.InsufficientData;
                return .{ .value = .{ .i16 = @bitCast(readU16(rest)) }, .consumed = 3 };
            },
            'u' => {
                if (rest.len < 2) return error.InsufficientData;
                return .{ .value = .{ .u16 = readU16(rest) }, .consumed = 3 };
            },
            'I' => {
                if (rest.len < 4) return error.InsufficientData;
                return .{ .value = .{ .i32 = @bitCast(readU32(rest)) }, .consumed = 5 };
            },
            'i' => {
                if (rest.len < 4) return error.InsufficientData;
                return .{ .value = .{ .u32 = readU32(rest) }, .consumed = 5 };
            },
            'l' => {
                if (rest.len < 8) return error.InsufficientData;
                return .{ .value = .{ .i64 = @bitCast(readU64(rest)) }, .consumed = 9 };
            },
            'f' => {
                if (rest.len < 4) return error.InsufficientData;
                return .{ .value = .{ .f32 = @bitCast(readU32(rest)) }, .consumed = 5 };
            },
            'd' => {
                if (rest.len < 8) return error.InsufficientData;
                return .{ .value = .{ .f64 = @bitCast(readU64(rest)) }, .consumed = 9 };
            },
            'D' => {
                if (rest.len < 5) return error.InsufficientData;
                return .{
                    .value = .{ .decimal = .{ .scale = rest[0], .value = readU32(rest[1..]) } },
                    .consumed = 6,
                };
            },
            'S' => {
                if (rest.len < 4) return error.InsufficientData;
                const len = readU32(rest);
                if (rest.len < 4 + len) return error.InsufficientData;
                const str = rest[4..][0..len];
                return .{ .value = .{ .long_string = str }, .consumed = 5 + len };
            },
            'T' => {
                if (rest.len < 8) return error.InsufficientData;
                return .{ .value = .{ .timestamp = readU64(rest) }, .consumed = 9 };
            },
            'F' => {
                const result = try FieldTable.decodeRaw(rest, allocator, depth + 1);
                return .{ .value = .{ .table = result.table }, .consumed = 1 + result.consumed };
            },
            'A' => {
                if (rest.len < 4) return error.InsufficientData;
                const array_len = readU32(rest);
                if (rest.len < 4 + array_len) return error.InsufficientData;
                var items: std.ArrayList(FieldValue) = .empty;
                defer items.deinit(allocator);
                var offset: usize = 4;
                const end = 4 + array_len;
                while (offset < end) {
                    const item_result = try FieldValue.decode(rest[offset..], allocator, depth + 1);
                    try items.append(allocator, item_result.value);
                    offset += item_result.consumed;
                }
                return .{ .value = .{ .array = try items.toOwnedSlice(allocator) }, .consumed = 1 + offset };
            },
            'x' => {
                if (rest.len < 4) return error.InsufficientData;
                const len = readU32(rest);
                if (rest.len < 4 + len) return error.InsufficientData;
                return .{ .value = .{ .byte_array = rest[4..][0..len] }, .consumed = 5 + len };
            },
            'V' => {
                return .{ .value = .{ .void = {} }, .consumed = 1 };
            },
            else => return error.UnknownFieldType,
        }
    }
};

pub const DecodeError = error{
    InsufficientData,
    TableNestingTooDeep,
    UnknownFieldType,
    OutOfMemory,
};

pub const DecodeResult = struct {
    value: FieldValue,
    consumed: usize,
};

pub const TableDecodeResult = struct {
    table: FieldTable,
    consumed: usize,
};

/// An AMQP field table: a map of string keys to field values.
pub const FieldTable = struct {
    entries: []Entry,
    allocator: Allocator,

    pub const Entry = struct {
        key: []const u8,
        value: FieldValue,
    };

    pub const empty = FieldTable{ .entries = &.{}, .allocator = undefined };

    pub fn init(allocator: Allocator) FieldTable {
        return .{ .entries = &.{}, .allocator = allocator };
    }

    pub fn deinit(self: *FieldTable) void {
        if (self.entries.len > 0) {
            self.allocator.free(self.entries);
        }
        self.entries = &.{};
    }

    /// Create a field table from a list of key-value pairs.
    pub fn fromEntries(allocator: Allocator, entries: []const Entry) !FieldTable {
        const owned = try allocator.dupe(Entry, entries);
        return .{ .entries = owned, .allocator = allocator };
    }

    /// Look up a value by key.
    pub fn get(self: FieldTable, key: []const u8) ?FieldValue {
        for (self.entries) |entry| {
            if (mem.eql(u8, entry.key, key)) return entry.value;
        }
        return null;
    }

    /// Encode the field table to wire format.
    pub fn encode(self: FieldTable, wb: *WireBuffer) void {
        const len_pos = wb.pos();
        wb.writeU32(0); // placeholder for table length
        const start = wb.pos();
        for (self.entries) |entry| {
            wb.writeShortString(entry.key);
            entry.value.encode(wb);
        }
        const end = wb.pos();
        wb.patchU32(len_pos, @intCast(end - start));
    }

    /// Decode a field table from raw bytes at a given nesting depth.
    pub fn decodeRaw(data: []const u8, allocator: Allocator, depth: u8) DecodeError!TableDecodeResult {
        if (depth > max_table_nesting) return error.TableNestingTooDeep;
        if (data.len < 4) return error.InsufficientData;

        const table_len = readU32(data);
        if (data.len < 4 + table_len) return error.InsufficientData;

        var entries: std.ArrayList(FieldTable.Entry) = .empty;
        defer entries.deinit(allocator);

        var offset: usize = 4;
        const end: usize = 4 + table_len;
        while (offset < end) {
            if (offset >= end) break;
            const key_len = data[offset];
            offset += 1;
            if (offset + key_len > end) return error.InsufficientData;
            const key = data[offset..][0..key_len];
            offset += key_len;

            const result = try FieldValue.decode(data[offset..], allocator, depth);
            try entries.append(allocator, .{ .key = key, .value = result.value });
            offset += result.consumed;
        }

        return .{
            .table = .{ .entries = try entries.toOwnedSlice(allocator), .allocator = allocator },
            .consumed = 4 + table_len,
        };
    }

    /// Decode from raw bytes starting at depth 0.
    pub fn decode(data: []const u8, allocator: Allocator) DecodeError!TableDecodeResult {
        return decodeRaw(data, allocator, 0);
    }
};

// Big-endian reading helpers
pub fn readU16(data: []const u8) u16 {
    return std.mem.readInt(u16, data[0..2], .big);
}

pub fn readU32(data: []const u8) u32 {
    return std.mem.readInt(u32, data[0..4], .big);
}

pub fn readU64(data: []const u8) u64 {
    return std.mem.readInt(u64, data[0..8], .big);
}

// Tests
test "field table encode/decode roundtrip" {
    const allocator = std.testing.allocator;

    const entries = [_]FieldTable.Entry{
        .{ .key = "bool_key", .value = .{ .boolean = true } },
        .{ .key = "int_key", .value = .{ .i32 = 42 } },
        .{ .key = "str_key", .value = .{ .long_string = "hello" } },
    };

    var table = try FieldTable.fromEntries(allocator, &entries);
    defer table.deinit();

    var buf: [1024]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    table.encode(&wb);

    const encoded = wb.getWritten();
    const result = try FieldTable.decode(encoded, allocator);
    var decoded = result.table;
    defer decoded.deinit();

    try std.testing.expectEqual(@as(usize, 3), decoded.entries.len);
    try std.testing.expect(decoded.get("bool_key").?.boolean == true);
    try std.testing.expect(decoded.get("int_key").?.i32 == 42);
    try std.testing.expectEqualSlices(u8, "hello", decoded.get("str_key").?.long_string);
}

test "field value void roundtrip" {
    const allocator = std.testing.allocator;

    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const v = FieldValue{ .void = {} };
    v.encode(&wb);

    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    try std.testing.expect(result.value == .void);
    try std.testing.expectEqual(@as(usize, 1), result.consumed);
}

test "field value timestamp" {
    const allocator = std.testing.allocator;

    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const v = FieldValue{ .timestamp = 1700000000 };
    v.encode(&wb);

    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    try std.testing.expectEqual(@as(u64, 1700000000), result.value.timestamp);
}

test "field value decimal" {
    const allocator = std.testing.allocator;

    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const v = FieldValue{ .decimal = .{ .scale = 2, .value = 12345 } };
    v.encode(&wb);

    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    try std.testing.expectEqual(@as(u8, 2), result.value.decimal.scale);
    try std.testing.expectEqual(@as(u32, 12345), result.value.decimal.value);
}

test "empty field table" {
    const allocator = std.testing.allocator;

    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const table = FieldTable.init(allocator);
    table.encode(&wb);

    const result = try FieldTable.decode(wb.getWritten(), allocator);
    try std.testing.expectEqual(@as(usize, 0), result.table.entries.len);
}

test "field value all numeric types roundtrip" {
    const allocator = std.testing.allocator;
    const cases = .{
        .{ FieldValue{ .i8 = -42 }, "i8" },
        .{ FieldValue{ .u8 = 200 }, "u8" },
        .{ FieldValue{ .i16 = -1000 }, "i16" },
        .{ FieldValue{ .u16 = 60000 }, "u16" },
        .{ FieldValue{ .i32 = -100000 }, "i32" },
        .{ FieldValue{ .u32 = 3_000_000_000 }, "u32" },
        .{ FieldValue{ .i64 = -9_000_000_000_000 }, "i64" },
        .{ FieldValue{ .f32 = 3.14 }, "f32" },
        .{ FieldValue{ .f64 = 2.718281828 }, "f64" },
    };

    inline for (cases) |case| {
        const v = case[0];
        var buf: [16]u8 = undefined;
        var wb = WireBuffer.init(&buf);
        v.encode(&wb);

        const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
        switch (v) {
            .i8 => |expected| try std.testing.expectEqual(expected, result.value.i8),
            .u8 => |expected| try std.testing.expectEqual(expected, result.value.u8),
            .i16 => |expected| try std.testing.expectEqual(expected, result.value.i16),
            .u16 => |expected| try std.testing.expectEqual(expected, result.value.u16),
            .i32 => |expected| try std.testing.expectEqual(expected, result.value.i32),
            .u32 => |expected| try std.testing.expectEqual(expected, result.value.u32),
            .i64 => |expected| try std.testing.expectEqual(expected, result.value.i64),
            .f32 => |expected| try std.testing.expectEqual(expected, result.value.f32),
            .f64 => |expected| try std.testing.expectEqual(expected, result.value.f64),
            else => unreachable,
        }
    }
}

test "field value byte_array roundtrip" {
    const allocator = std.testing.allocator;
    const data = "\x00\x01\x02\xff\xfe";

    var buf: [64]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const v = FieldValue{ .byte_array = data };
    v.encode(&wb);

    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    try std.testing.expectEqualSlices(u8, data, result.value.byte_array);
}

test "field value array roundtrip" {
    const allocator = std.testing.allocator;
    const items = [_]FieldValue{
        .{ .boolean = true },
        .{ .i32 = 42 },
        .{ .long_string = "hello" },
        .{ .void = {} },
    };

    var buf: [256]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const v = FieldValue{ .array = &items };
    v.encode(&wb);

    const result = try FieldValue.decode(wb.getWritten(), allocator, 0);
    defer allocator.free(result.value.array);
    try std.testing.expectEqual(@as(usize, 4), result.value.array.len);
    try std.testing.expect(result.value.array[0].boolean == true);
    try std.testing.expect(result.value.array[1].i32 == 42);
    try std.testing.expectEqualSlices(u8, "hello", result.value.array[2].long_string);
    try std.testing.expect(result.value.array[3] == .void);
}

test "field table with all value types" {
    const allocator = std.testing.allocator;
    const entries = [_]FieldTable.Entry{
        .{ .key = "a_bool", .value = .{ .boolean = false } },
        .{ .key = "b_i32", .value = .{ .i32 = -999 } },
        .{ .key = "c_str", .value = .{ .long_string = "test string" } },
        .{ .key = "d_ts", .value = .{ .timestamp = 1710000000 } },
        .{ .key = "e_void", .value = .{ .void = {} } },
        .{ .key = "f_u8", .value = .{ .u8 = 255 } },
        .{ .key = "g_f64", .value = .{ .f64 = 1.5 } },
    };

    var table = try FieldTable.fromEntries(allocator, &entries);
    defer table.deinit();

    var buf: [1024]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    table.encode(&wb);

    const result = try FieldTable.decode(wb.getWritten(), allocator);
    var decoded = result.table;
    defer decoded.deinit();

    try std.testing.expectEqual(@as(usize, 7), decoded.entries.len);
    try std.testing.expect(decoded.get("a_bool").?.boolean == false);
    try std.testing.expect(decoded.get("b_i32").?.i32 == -999);
    try std.testing.expectEqualSlices(u8, "test string", decoded.get("c_str").?.long_string);
    try std.testing.expect(decoded.get("d_ts").?.timestamp == 1710000000);
    try std.testing.expect(decoded.get("e_void").? == .void);
    try std.testing.expect(decoded.get("f_u8").?.u8 == 255);
    try std.testing.expect(decoded.get("g_f64").?.f64 == 1.5);
}

test "field table key not found returns null" {
    const allocator = std.testing.allocator;
    const entries = [_]FieldTable.Entry{
        .{ .key = "exists", .value = .{ .boolean = true } },
    };
    var table = try FieldTable.fromEntries(allocator, &entries);
    defer table.deinit();

    try std.testing.expect(table.get("exists") != null);
    try std.testing.expect(table.get("missing") == null);
}

test "nested field table" {
    const allocator = std.testing.allocator;

    const inner_entries = [_]FieldTable.Entry{
        .{ .key = "nested", .value = .{ .i32 = 99 } },
    };
    var inner = try FieldTable.fromEntries(allocator, &inner_entries);
    defer inner.deinit();

    const outer_entries = [_]FieldTable.Entry{
        .{ .key = "inner", .value = .{ .table = inner } },
    };
    var outer = try FieldTable.fromEntries(allocator, &outer_entries);
    defer outer.deinit();

    var buf: [1024]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    outer.encode(&wb);

    const result = try FieldTable.decode(wb.getWritten(), allocator);
    var decoded = result.table;
    defer decoded.deinit();

    var inner_val = decoded.get("inner").?.table;
    defer inner_val.deinit();
    try std.testing.expectEqual(@as(i32, 99), inner_val.get("nested").?.i32);
}

test "fuzz: field table decode does not crash on arbitrary input" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *std.testing.Smith) !void {
            var buf: [256]u8 = undefined;
            const len = smith.sliceWithHash(&buf, 0);
            const allocator = std.testing.allocator;
            const result = FieldTable.decode(buf[0..len], allocator) catch return;
            var table = result.table;
            table.deinit();
        }
    }.f, .{});
}

test "fuzz: field value decode does not crash on arbitrary input" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, smith: *std.testing.Smith) !void {
            var buf: [256]u8 = undefined;
            const len = smith.sliceWithHash(&buf, 0);
            const allocator = std.testing.allocator;
            _ = FieldValue.decode(buf[0..len], allocator, 0) catch return;
        }
    }.f, .{});
}
