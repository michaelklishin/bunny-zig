//! Property-based tests for `src/protocol/wire.zig`.
const std = @import("std");
const pt = @import("proptest");
const bunny = @import("bunny");

const wire = bunny.protocol.wire;
const WireBuffer = wire.WireBuffer;
const WireReader = wire.WireReader;

const allocator = std.testing.allocator;

fn u16Roundtrip(v: u16) !void {
    var buf: [4]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeU16(v);
    var r = WireReader.init(wb.getWritten());
    if ((try r.readU16()) != v) return error.RoundtripMismatch;
    if (r.remaining() != 0) return error.UnconsumedBytes;
}

fn u32Roundtrip(v: u32) !void {
    var buf: [8]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeU32(v);
    var r = WireReader.init(wb.getWritten());
    if ((try r.readU32()) != v) return error.RoundtripMismatch;
    if (r.remaining() != 0) return error.UnconsumedBytes;
}

fn u64Roundtrip(v: u64) !void {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeU64(v);
    var r = WireReader.init(wb.getWritten());
    if ((try r.readU64()) != v) return error.RoundtripMismatch;
    if (r.remaining() != 0) return error.UnconsumedBytes;
}

fn i32Roundtrip(v: i32) !void {
    var buf: [8]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeI32(v);
    var r = WireReader.init(wb.getWritten());
    if ((try r.readI32()) != v) return error.RoundtripMismatch;
    if (r.remaining() != 0) return error.UnconsumedBytes;
}

fn i64Roundtrip(v: i64) !void {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeI64(v);
    var r = WireReader.init(wb.getWritten());
    if ((try r.readI64()) != v) return error.RoundtripMismatch;
    if (r.remaining() != 0) return error.UnconsumedBytes;
}

fn writesBigEndian(v: u32) !void {
    var buf: [4]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeU32(v);
    const expected: u32 =
        @as(u32, buf[0]) << 24 |
        @as(u32, buf[1]) << 16 |
        @as(u32, buf[2]) << 8 |
        @as(u32, buf[3]);
    if (expected != v) return error.WrongEndianness;
}

fn shortStringRoundtrip(s: []const u8) !void {
    var buf: [300]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeShortString(s);
    if (wb.getWritten().len != 1 + s.len) return error.WrongEncodedLength;
    var r = WireReader.init(wb.getWritten());
    const decoded = try r.readShortString();
    if (!std.mem.eql(u8, s, decoded)) return error.RoundtripMismatch;
    if (r.remaining() != 0) return error.UnconsumedBytes;
}

fn longStringRoundtrip(s: []const u8) !void {
    var buf: [4200]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeLongString(s);
    if (wb.getWritten().len != 4 + s.len) return error.WrongEncodedLength;
    var r = WireReader.init(wb.getWritten());
    const decoded = try r.readLongString();
    if (!std.mem.eql(u8, s, decoded)) return error.RoundtripMismatch;
    if (r.remaining() != 0) return error.UnconsumedBytes;
}

fn patchU32Idempotent(p: struct { u32, u32 }) !void {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const patch_pos = wb.pos();
    wb.writeU32(p[0]);
    wb.writeU16(0xBEEF);
    wb.patchU32(patch_pos, p[1]);

    var r = WireReader.init(wb.getWritten());
    if ((try r.readU32()) != p[1]) return error.PatchedValueMismatch;
    if ((try r.readU16()) != 0xBEEF) return error.TrailerCorrupted;
}

fn truncatedReadFails(p: struct { u32, u8 }) !void {
    var buf: [4]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeU32(p[0]);

    const cut: usize = p[1] % 4; // always strictly less than 4 bytes
    var r = WireReader.init(wb.getWritten()[0..cut]);
    const result = r.readU32();
    if (result) |_| return error.ShouldHaveFailed else |err| {
        if (err != error.InsufficientData) return error.WrongError;
    }
}

test "wire prop: u16 encode/decode roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(u16), u16Roundtrip);
}

test "wire prop: u32 encode/decode roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(u32), u32Roundtrip);
}

test "wire prop: u64 encode/decode roundtrip" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(u64), u64Roundtrip);
}

test "wire prop: i32 encode/decode roundtrip across full range" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(i32), i32Roundtrip);
}

test "wire prop: i64 encode/decode roundtrip across full range" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(i64), i64Roundtrip);
}

test "wire prop: u32 is encoded big-endian" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.num.int(u32), writesBigEndian);
}

test "wire prop: short string roundtrip across all valid lengths" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.bytes(0, 255), shortStringRoundtrip);
}

test "wire prop: long string roundtrip including binary content" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.bytes(0, 4096), longStringRoundtrip);
}

test "wire prop: patchU32 overwrites the placeholder without disturbing trailer" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const pair = pt.tuple.t2(pt.num.int(u32), pt.num.int(u32));
    try runner.check(allocator, pair, patchU32Idempotent);
}

test "wire prop: truncated read returns InsufficientData" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const pair = pt.tuple.t2(pt.num.int(u32), pt.num.int(u8));
    try runner.check(allocator, pair, truncatedReadFails);
}
