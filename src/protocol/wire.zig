/// Low-level wire format encoding/decoding helpers.
const std = @import("std");

/// A buffer for encoding AMQP wire data.
pub const WireBuffer = struct {
    buf: []u8,
    offset: usize = 0,

    pub fn init(buf: []u8) WireBuffer {
        return .{ .buf = buf, .offset = 0 };
    }

    pub fn pos(self: *const WireBuffer) usize {
        return self.offset;
    }

    pub fn remaining(self: *const WireBuffer) usize {
        return self.buf.len - self.offset;
    }

    pub fn getWritten(self: *const WireBuffer) []const u8 {
        return self.buf[0..self.offset];
    }

    pub fn writeByte(self: *WireBuffer, b: u8) void {
        self.buf[self.offset] = b;
        self.offset += 1;
    }

    pub fn writeBytes(self: *WireBuffer, data: []const u8) void {
        @memcpy(self.buf[self.offset..][0..data.len], data);
        self.offset += data.len;
    }

    pub fn writeI8(self: *WireBuffer, v: i8) void {
        self.buf[self.offset] = @bitCast(v);
        self.offset += 1;
    }

    pub fn writeU16(self: *WireBuffer, v: u16) void {
        std.mem.writeInt(u16, self.buf[self.offset..][0..2], v, .big);
        self.offset += 2;
    }

    pub fn writeI16(self: *WireBuffer, v: i16) void {
        self.writeU16(@bitCast(v));
    }

    pub fn writeU32(self: *WireBuffer, v: u32) void {
        std.mem.writeInt(u32, self.buf[self.offset..][0..4], v, .big);
        self.offset += 4;
    }

    pub fn writeI32(self: *WireBuffer, v: i32) void {
        self.writeU32(@bitCast(v));
    }

    pub fn writeU64(self: *WireBuffer, v: u64) void {
        std.mem.writeInt(u64, self.buf[self.offset..][0..8], v, .big);
        self.offset += 8;
    }

    pub fn writeI64(self: *WireBuffer, v: i64) void {
        self.writeU64(@bitCast(v));
    }

    pub fn writeF32(self: *WireBuffer, v: f32) void {
        self.writeU32(@bitCast(v));
    }

    pub fn writeF64(self: *WireBuffer, v: f64) void {
        self.writeU64(@bitCast(v));
    }

    /// Write a short string (max 255 bytes): u8 length prefix.
    pub fn writeShortString(self: *WireBuffer, s: []const u8) void {
        std.debug.assert(s.len <= 255);
        self.writeByte(@intCast(s.len));
        self.writeBytes(s);
    }

    /// Write a long string: u32 length prefix.
    pub fn writeLongString(self: *WireBuffer, s: []const u8) void {
        self.writeU32(@intCast(s.len));
        self.writeBytes(s);
    }

    /// Patch a u32 value at a specific offset (for back-patching lengths).
    pub fn patchU32(self: *WireBuffer, offset: usize, v: u32) void {
        std.mem.writeInt(u32, self.buf[offset..][0..4], v, .big);
    }

    /// Patch a u16 value at a specific offset.
    pub fn patchU16(self: *WireBuffer, offset: usize, v: u16) void {
        std.mem.writeInt(u16, self.buf[offset..][0..2], v, .big);
    }
};

/// A reader for decoding AMQP wire data.
pub const WireReader = struct {
    data: []const u8,
    offset: usize = 0,

    pub fn init(data: []const u8) WireReader {
        return .{ .data = data, .offset = 0 };
    }

    pub fn remaining(self: *const WireReader) usize {
        return self.data.len - self.offset;
    }

    pub fn readByte(self: *WireReader) !u8 {
        if (self.offset >= self.data.len) return error.InsufficientData;
        const b = self.data[self.offset];
        self.offset += 1;
        return b;
    }

    pub fn readBytes(self: *WireReader, n: usize) ![]const u8 {
        if (self.offset + n > self.data.len) return error.InsufficientData;
        const slice = self.data[self.offset..][0..n];
        self.offset += n;
        return slice;
    }

    pub fn readU16(self: *WireReader) !u16 {
        const bytes = try self.readBytes(2);
        return std.mem.readInt(u16, bytes[0..2], .big);
    }

    pub fn readU32(self: *WireReader) !u32 {
        const bytes = try self.readBytes(4);
        return std.mem.readInt(u32, bytes[0..4], .big);
    }

    pub fn readU64(self: *WireReader) !u64 {
        const bytes = try self.readBytes(8);
        return std.mem.readInt(u64, bytes[0..8], .big);
    }

    pub fn readI32(self: *WireReader) !i32 {
        return @bitCast(try self.readU32());
    }

    pub fn readI64(self: *WireReader) !i64 {
        return @bitCast(try self.readU64());
    }

    pub fn readShortString(self: *WireReader) ![]const u8 {
        const len = try self.readByte();
        return self.readBytes(len);
    }

    pub fn readLongString(self: *WireReader) ![]const u8 {
        const len = try self.readU32();
        return self.readBytes(len);
    }

    /// Read boolean flags packed into bits of a byte.
    pub fn readBool(self: *WireReader) !bool {
        const b = try self.readByte();
        return b != 0;
    }

    pub fn rest(self: *const WireReader) []const u8 {
        return self.data[self.offset..];
    }
};

// Tests
test "WireBuffer basic encoding" {
    var buf: [64]u8 = undefined;
    var wb = WireBuffer.init(&buf);

    wb.writeU16(0x1234);
    wb.writeU32(0xDEADBEEF);
    wb.writeByte(0xFF);
    wb.writeShortString("hello");

    const written = wb.getWritten();
    try std.testing.expectEqual(2 + 4 + 1 + 1 + 5, written.len);

    var r = WireReader.init(written);
    try std.testing.expectEqual(0x1234, try r.readU16());
    try std.testing.expectEqual(0xDEADBEEF, try r.readU32());
    try std.testing.expectEqual(0xFF, try r.readByte());
    try std.testing.expectEqualSlices(u8, "hello", try r.readShortString());
}

test "WireBuffer long string" {
    var buf: [256]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    wb.writeLongString("a long string value");

    var r = WireReader.init(wb.getWritten());
    try std.testing.expectEqualSlices(u8, "a long string value", try r.readLongString());
}

test "WireBuffer patch" {
    var buf: [16]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const patch_pos = wb.pos();
    wb.writeU32(0);
    wb.writeU16(42);
    wb.patchU32(patch_pos, 12345);

    var r = WireReader.init(wb.getWritten());
    try std.testing.expectEqual(12345, try r.readU32());
    try std.testing.expectEqual(42, try r.readU16());
}
