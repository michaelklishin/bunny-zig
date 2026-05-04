/// TCP and TLS transport for AMQP 0-9-1 connections.
const std = @import("std");
const Io = std.Io;
const net = Io.net;
const posix = std.posix;
const tls = @import("tls");
const protocol = @import("protocol.zig");
const frame_mod = protocol.frame;
const constants = protocol.constants;

const log = std.log.scoped(.bunny_transport);
const StreamReader = net.Stream.Reader;
const StreamWriter = net.Stream.Writer;

/// Get a usable Io context for operations.
fn getIo() Io {
    return Io.Threaded.global_single_threaded.io();
}

/// Connect to a host by IP literal or hostname.
fn connectToHost(io: Io, host: []const u8, port: u16, timeout: Io.Timeout) !net.Stream {
    // Try as an IP literal first, fall back to DNS resolution for hostnames
    if (net.IpAddress.parse(host, port)) |address| {
        return net.IpAddress.connect(&address, io, .{ .mode = .stream, .timeout = timeout });
    } else |_| {
        const hostname = try net.HostName.init(host);
        return hostname.connect(io, port, .{ .mode = .stream, .timeout = timeout });
    }
}

pub const TransportError = error{
    ConnectionRefused,
    ConnectionReset,
    BrokenPipe,
    Timeout,
    EndOfStream,
    InvalidFrameEnd,
    UnknownFrameType,
    UnknownMethod,
    UnknownFieldType,
    InsufficientData,
    TableNestingTooDeep,
    TlsHandshakeFailed,
    Unexpected,
} || std.mem.Allocator.Error || posix.ReadError;

pub const TlsOptions = struct {
    /// Hostname for SNI and certificate verification.
    host: []const u8 = "localhost",
    /// Path to a PEM CA bundle for chain validation.
    ca_file: ?[]const u8 = null,
    /// Skip TLS peer certificate chain verification. DO NOT use in production.
    skip_peer_certificate_chain_verification: bool = false,
};

/// Low-level transport for sending and receiving AMQP 0-9-1 frames over TCP or TLS.
pub const Transport = struct {
    stream: net.Stream,
    tls_conn: ?tls.Connection = null,
    read_buf: []u8,
    write_buf: []u8,
    // Heap-allocated IO wrappers (must outlive tls_conn)
    io_reader: ?*StreamReader = null,
    io_writer: ?*StreamWriter = null,
    // Heap-allocated RNG source (must outlive tls_conn for TLS 1.2 IV generation)
    rng_source: ?*std.Random.IoSource = null,
    // Stream reader/writer for plain TCP
    stream_reader: ?StreamReader = null,
    stream_writer: ?StreamWriter = null,
    read_start: usize = 0,
    read_end: usize = 0,
    allocator: std.mem.Allocator,
    closed: bool = false,

    pub fn connect(allocator: std.mem.Allocator, host: []const u8, port: u16, timeout: std.Io.Timeout) !Transport {
        const io = getIo();
        const stream = try connectToHost(io, host, port, timeout);
        errdefer stream.close(io);

        posix.setsockopt(stream.socket.handle, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1))) catch {};

        const buf_size = 256 * 1024;
        const read_buf = try allocator.alloc(u8, buf_size);
        errdefer allocator.free(read_buf);
        const write_buf = try allocator.alloc(u8, 64 * 1024);
        errdefer allocator.free(write_buf);

        const io_read_buf = try allocator.alloc(u8, 64 * 1024);

        return .{
            .stream = stream,
            .read_buf = read_buf,
            .write_buf = write_buf,
            .stream_reader = stream.reader(io, io_read_buf),
            .stream_writer = stream.writer(io, write_buf),
            .allocator = allocator,
        };
    }

    /// Connect with TLS using ianic/tls.zig (supports TLS 1.2 and 1.3).
    pub fn connectTls(allocator: std.mem.Allocator, host: []const u8, port: u16, tls_opts: TlsOptions, timeout: std.Io.Timeout) !Transport {
        const io = getIo();
        if (tls_opts.skip_peer_certificate_chain_verification) {
            log.warn("TLS to {s}:{d} skipping peer certificate chain verification", .{ host, port });
        }
        const stream = try connectToHost(io, host, port, timeout);
        errdefer stream.close(io);

        posix.setsockopt(stream.socket.handle, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1))) catch {};

        const buf_size = 256 * 1024;
        const read_buf = try allocator.alloc(u8, buf_size);
        errdefer allocator.free(read_buf);

        // Stream IO buffers (for reading/writing encrypted data on the wire)
        const stream_read_buf = try allocator.alloc(u8, tls.input_buffer_len);
        errdefer allocator.free(stream_read_buf);
        const stream_write_buf = try allocator.alloc(u8, tls.output_buffer_len);
        errdefer allocator.free(stream_write_buf);

        // Heap-allocate the Reader and Writer so the TLS connection's pointers
        // remain valid after this function returns.
        const reader = try allocator.create(StreamReader);
        errdefer allocator.destroy(reader);
        reader.* = StreamReader.init(stream, io, stream_read_buf);

        const writer = try allocator.create(StreamWriter);
        errdefer allocator.destroy(writer);
        writer.* = StreamWriter.init(stream, io, stream_write_buf);

        // Load CA bundle if provided, otherwise skip verification
        var ca_bundle: std.crypto.Certificate.Bundle = .empty;
        var owns_ca_bundle = false;
        if (tls_opts.ca_file) |ca_path| {
            ca_bundle = tls.config.cert.fromFilePathAbsolute(allocator, io, ca_path) catch |err| {
                log.err("Failed to load CA file '{s}': {}", .{ ca_path, err });
                return error.TlsHandshakeFailed;
            };
            owns_ca_bundle = true;
        }
        defer if (owns_ca_bundle) ca_bundle.deinit(allocator);

        // Heap-allocate the RNG source so the cipher's pointer remains valid
        // after this function returns (needed for TLS 1.2 IV generation).
        const rng_source = try allocator.create(std.Random.IoSource);
        errdefer allocator.destroy(rng_source);
        rng_source.* = .{ .io = io };

        const tls_conn = tls.client(&reader.interface, &writer.interface, .{
            .host = tls_opts.host,
            .root_ca = ca_bundle,
            .insecure_skip_verify = tls_opts.skip_peer_certificate_chain_verification,
            .rng = rng_source.interface(),
            .now = Io.Clock.real.now(io),
        }) catch |err| {
            log.err("TLS handshake failed: {}", .{err});
            return error.TlsHandshakeFailed;
        };

        return .{
            .stream = stream,
            .tls_conn = tls_conn,
            .read_buf = read_buf,
            .write_buf = &.{},
            .io_reader = reader,
            .io_writer = writer,
            .rng_source = rng_source,
            .allocator = allocator,
        };
    }

    pub fn close(self: *Transport) void {
        if (self.closed) return;
        self.closed = true;
        if (self.tls_conn) |*tc| {
            tc.close() catch {};
        }
        self.stream.close(getIo());
        self.allocator.free(self.read_buf);
        if (self.write_buf.len > 0) self.allocator.free(self.write_buf);
        if (self.stream_reader) |r| self.allocator.free(r.interface.buffer);
        if (self.io_reader) |r| {
            self.allocator.free(r.interface.buffer);
            self.allocator.destroy(r);
        }
        if (self.io_writer) |w| {
            self.allocator.free(w.interface.buffer);
            self.allocator.destroy(w);
        }
        if (self.rng_source) |r| self.allocator.destroy(r);
    }

    /// Get the abstract Io.Reader interface for this transport.
    /// For TLS connections, returns the TLS decrypted reader.
    /// For plain TCP, returns the stream's reader interface.
    pub fn ioReader(self: *Transport) ?*Io.Reader {
        if (self.tls_conn) |*tc| {
            return tc.input;
        }
        if (self.io_reader) |r| return &r.interface;
        return null;
    }

    /// Get the abstract Io.Writer interface for this transport.
    pub fn ioWriter(self: *Transport) ?*Io.Writer {
        if (self.tls_conn) |*tc| {
            return tc.output;
        }
        if (self.io_writer) |w| return &w.interface;
        return null;
    }

    pub fn sendProtocolHeader(self: *Transport) !void {
        try self.writeAll(constants.protocol_header);
    }

    /// Send a raw frame and flush.
    pub fn sendFrame(self: *Transport, frame: frame_mod.Frame) !void {
        var buf: [constants.default_frame_max + constants.frame_overhead]u8 = undefined;
        const encoded = frame_mod.encodeFrame(&buf, frame);
        try self.writeAll(encoded);
    }

    /// Send a raw frame without flushing.
    pub fn sendFrameNoFlush(self: *Transport, frame: frame_mod.Frame) !void {
        var buf: [constants.default_frame_max + constants.frame_overhead]u8 = undefined;
        const encoded = frame_mod.encodeFrame(&buf, frame);
        try self.writeNoFlush(encoded);
    }

    /// Send a method frame on a specific channel.
    pub fn sendMethod(self: *Transport, channel_id: u16, method: protocol.method.Method) !void {
        try self.sendFrame(.{ .method = .{ .channel = channel_id, .method = method } });
    }

    /// Send content: header frame followed by body frames (split by frame_max).
    pub fn sendContent(self: *Transport, channel_id: u16, props: protocol.properties.BasicProperties, body: []const u8, frame_max: u32) !void {
        var header_buf: [constants.default_frame_max + constants.frame_overhead]u8 = undefined;
        const header_encoded = frame_mod.encodeFrame(&header_buf, .{ .header = .{
            .channel = channel_id,
            .class_id = constants.class_basic,
            .body_size = body.len,
            .properties = props,
        } });
        try self.writeNoFlush(header_encoded);

        // AMQP 0-9-1: no body frames when body_size is 0
        if (body.len > 0) {
            const max_body_per_frame = frame_max - constants.frame_overhead;
            var offset: usize = 0;
            while (offset < body.len) {
                const end = @min(offset + max_body_per_frame, body.len);
                var body_buf: [constants.default_frame_max + constants.frame_overhead]u8 = undefined;
                const body_encoded = frame_mod.encodeFrame(&body_buf, .{ .body = .{
                    .channel = channel_id,
                    .payload = body[offset..end],
                } });
                try self.writeNoFlush(body_encoded);
                offset = end;
            }
        }
        try self.flush();
    }

    /// Send a complete publish (method + header + body frames) in a single flush.
    pub fn sendPublish(
        self: *Transport,
        channel_id: u16,
        method: protocol.method.Method,
        props: protocol.properties.BasicProperties,
        body: []const u8,
        frame_max: u32,
        do_flush: bool,
    ) !void {
        // Estimate total size: method frame + header frame + body frames
        const max_body_per_frame = frame_max - constants.frame_overhead;
        const body_frame_count = if (body.len == 0) 0 else (body.len + max_body_per_frame - 1) / max_body_per_frame;
        const estimated_size = 512 + // method frame (publish is small)
            256 + // header frame (properties)
            body.len + (body_frame_count * constants.frame_overhead);

        // Use stack buffer for small messages, heap for large
        var heap_buf: ?[]u8 = null;
        defer if (heap_buf) |hb| self.allocator.free(hb);

        // Stack buffer sized for one body frame + method + header overhead.
        // Messages up to ~130KB use the stack. Larger ones heap-allocate.
        var buf: [constants.default_frame_max + 1024]u8 = undefined;
        const write_buf = if (estimated_size <= buf.len)
            &buf
        else blk: {
            heap_buf = self.allocator.alloc(u8, estimated_size) catch {
                // Fallback to separate writes
                try self.sendFrame(.{ .method = .{ .channel = channel_id, .method = method } });
                try self.sendContent(channel_id, props, body, frame_max);
                return;
            };
            break :blk heap_buf.?;
        };

        var offset: usize = 0;

        // Encode method frame
        const method_encoded = frame_mod.encodeFrame(write_buf[offset..], .{ .method = .{ .channel = channel_id, .method = method } });
        offset += method_encoded.len;

        // Encode header frame
        const header_encoded = frame_mod.encodeFrame(write_buf[offset..], .{ .header = .{
            .channel = channel_id,
            .class_id = constants.class_basic,
            .body_size = body.len,
            .properties = props,
        } });
        offset += header_encoded.len;

        // AMQP 0-9-1: no body frames when body_size is 0
        if (body.len > 0) {
            var body_offset: usize = 0;
            while (body_offset < body.len) {
                const end = @min(body_offset + max_body_per_frame, body.len);
                const body_encoded = frame_mod.encodeFrame(write_buf[offset..], .{ .body = .{
                    .channel = channel_id,
                    .payload = body[body_offset..end],
                } });
                offset += body_encoded.len;
                body_offset = end;
            }
        }

        try self.writeNoFlush(write_buf[0..offset]);
        if (do_flush) try self.flush();
    }

    /// Send a batch of publishes with the same method and properties.
    /// Encodes the method+header template once, patches body_size per message,
    /// and accumulates messages into a staging buffer to minimize writeNoFlush calls.
    pub fn sendPublishBatch(
        self: *Transport,
        channel_id: u16,
        method: protocol.method.Method,
        props: protocol.properties.BasicProperties,
        bodies: []const []const u8,
        frame_max: u32,
        do_flush: bool,
    ) !void {
        if (bodies.len == 0) return;

        const max_body_per_frame = frame_max - constants.frame_overhead;

        // Encode method + header as a contiguous template (~50 bytes).
        var template_store: [512]u8 = undefined;
        const method_bytes = frame_mod.encodeFrame(&template_store, .{ .method = .{
            .channel = channel_id,
            .method = method,
        } });
        const method_len = method_bytes.len;

        const header_bytes = frame_mod.encodeFrame(template_store[method_len..], .{ .header = .{
            .channel = channel_id,
            .class_id = constants.class_basic,
            .body_size = 0,
            .properties = props,
        } });
        const header_len = header_bytes.len;
        const template_len = method_len + header_len;

        // body_size offset: method_len + type(1) + channel(2) + size(4) + class(2) + weight(2)
        const body_size_offset = method_len + 11;

        // Staging buffer: accumulate multiple messages before writing.
        // Sized to match the transport write buffer so each writeNoFlush
        // fills it in one pass.
        var staging: [64 * 1024]u8 = undefined;
        var cursor: usize = 0;

        for (bodies) |body| {
            std.mem.writeInt(u64, template_store[body_size_offset..][0..8], @intCast(body.len), .big);

            if (body.len == 0) {
                // Empty body: method + header only, no body frame
                if (cursor + template_len > staging.len) {
                    try self.writeNoFlush(staging[0..cursor]);
                    cursor = 0;
                }
                @memcpy(staging[cursor..][0..template_len], template_store[0..template_len]);
                cursor += template_len;
            } else if (body.len <= max_body_per_frame) {
                // Common case: single body frame
                const msg_len = template_len + constants.frame_overhead + body.len;
                if (msg_len > staging.len) {
                    // Message too large for staging, flush and write directly
                    if (cursor > 0) {
                        try self.writeNoFlush(staging[0..cursor]);
                        cursor = 0;
                    }
                    try self.writeNoFlush(template_store[0..template_len]);
                    var body_frame_buf: [constants.default_frame_max + constants.frame_overhead]u8 = undefined;
                    const encoded = frame_mod.encodeFrame(&body_frame_buf, .{ .body = .{
                        .channel = channel_id,
                        .payload = body,
                    } });
                    try self.writeNoFlush(encoded);
                } else {
                    if (cursor + msg_len > staging.len) {
                        try self.writeNoFlush(staging[0..cursor]);
                        cursor = 0;
                    }
                    // Copy template (method + header)
                    @memcpy(staging[cursor..][0..template_len], template_store[0..template_len]);
                    cursor += template_len;
                    // Inline body frame: type(1) + channel(2) + size(4) + payload + end(1)
                    staging[cursor] = constants.frame_body;
                    cursor += 1;
                    std.mem.writeInt(u16, staging[cursor..][0..2], channel_id, .big);
                    cursor += 2;
                    std.mem.writeInt(u32, staging[cursor..][0..4], @intCast(body.len), .big);
                    cursor += 4;
                    @memcpy(staging[cursor..][0..body.len], body);
                    cursor += body.len;
                    staging[cursor] = constants.frame_end;
                    cursor += 1;
                }
            } else {
                // Large body spanning multiple frames
                if (cursor > 0) {
                    try self.writeNoFlush(staging[0..cursor]);
                    cursor = 0;
                }
                try self.writeNoFlush(template_store[0..template_len]);
                var body_off: usize = 0;
                while (body_off < body.len) {
                    const end = @min(body_off + max_body_per_frame, body.len);
                    var body_frame_buf: [constants.default_frame_max + constants.frame_overhead]u8 = undefined;
                    const encoded = frame_mod.encodeFrame(&body_frame_buf, .{ .body = .{
                        .channel = channel_id,
                        .payload = body[body_off..end],
                    } });
                    try self.writeNoFlush(encoded);
                    body_off = end;
                }
            }
        }

        if (cursor > 0) try self.writeNoFlush(staging[0..cursor]);
        if (do_flush) try self.flush();
    }

    /// Read the next complete frame from the connection.
    pub fn readFrame(self: *Transport, allocator: std.mem.Allocator) !frame_mod.Frame {
        while (true) {
            const available = self.read_buf[self.read_start..self.read_end];
            if (available.len >= constants.frame_overhead) {
                const maybe_frame = try frame_mod.decodeFrame(available, allocator);
                if (maybe_frame) |result| {
                    self.read_start += result.consumed;
                    if (self.read_start > self.read_buf.len / 2) {
                        self.compactBuffer();
                    }
                    return result.frame;
                }
            }

            self.compactBuffer();
            const bytes_read = self.readFromStream(self.read_buf[self.read_end..]) catch return error.EndOfStream;
            if (bytes_read == 0) return error.EndOfStream;
            self.read_end += bytes_read;
        }
    }

    fn readFromStream(self: *Transport, buf: []u8) !usize {
        if (self.tls_conn) |*tc| {
            return tc.read(buf) catch return error.EndOfStream;
        }
        if (self.stream_reader) |*rdr| {
            var bufs = [_][]u8{buf};
            return rdr.interface.readVec(&bufs) catch return error.EndOfStream;
        }
        return error.EndOfStream;
    }

    fn compactBuffer(self: *Transport) void {
        if (self.read_start > 0) {
            const remaining = self.read_end - self.read_start;
            if (remaining > 0) {
                std.mem.copyForwards(u8, self.read_buf[0..remaining], self.read_buf[self.read_start..self.read_end]);
            }
            self.read_end = remaining;
            self.read_start = 0;
        }
    }

    fn writeAll(self: *Transport, data: []const u8) !void {
        try self.writeNoFlush(data);
        try self.flush();
    }

    /// Write data to the transport buffer without flushing to the socket.
    fn writeNoFlush(self: *Transport, data: []const u8) !void {
        if (self.tls_conn) |*tc| {
            tc.writeAll(data) catch return error.EndOfStream;
            return;
        }
        if (self.stream_writer) |*w| {
            w.interface.writeAll(data) catch return error.EndOfStream;
        } else {
            return error.EndOfStream;
        }
    }

    /// Flush the transport buffer to the socket.
    pub fn flush(self: *Transport) !void {
        if (self.tls_conn != null) return; // TLS flushes on writeAll
        if (self.stream_writer) |*w| {
            w.interface.flush() catch return error.EndOfStream;
        }
    }

    /// Shut down the socket for both reading and writing.
    /// This unblocks any threads in blocking readv/writev (they see EOF)
    /// without closing the fd, so Zig's IO layer won't panic on BADF.
    pub fn shutdownSocket(self: *Transport) void {
        self.stream.shutdown(getIo(), .both) catch {};
    }
};
