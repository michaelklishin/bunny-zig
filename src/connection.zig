/// AMQP 0-9-1 connection management.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Thread = std.Thread;
const Io = std.Io;
const Mutex = Io.Mutex;
const Condition = Io.Condition;
const Notify = @import("notify.zig").Notify;
const posix = std.posix;

/// Get a usable Io context for mutex/condition/sleep operations.
fn getIo() Io {
    return Io.Threaded.global_single_threaded.io();
}

const protocol = @import("protocol.zig");
const constants = protocol.constants;
const frame_mod = protocol.frame;
const method_mod = protocol.method;
const Method = method_mod.Method;
const types = protocol.types;
const FieldTable = types.FieldTable;
const transport_mod = @import("transport.zig");
const Transport = transport_mod.Transport;
const TlsOptions = transport_mod.TlsOptions;
const Channel = @import("channel.zig").Channel;
const events = @import("events.zig");
const ConnectionEvent = events.ConnectionEvent;
const recovery_mod = @import("recovery.zig");

const log = std.log.scoped(.bunny_connection);

pub const ConnectError = error{
    ConnectionRefused,
    ConnectionReset,
    TlsHandshakeFailed,
    EndOfStream,
    Unexpected,
    BrokenPipe,
} || Allocator.Error;

pub const HandshakeError = ConnectError || error{
    AuthenticationFailed,
    ConnectionClosed,
    ProtocolError,
    InvalidFrameEnd,
    UnknownMethod,
    InsufficientData,
    UnknownFieldType,
    TableNestingTooDeep,
    UnknownFrameType,
};

pub const ChannelOpenError = HandshakeError || error{
    NotConnected,
    ChannelLimitReached,
    ChannelClosed,
    Timeout,
};

pub const ChannelError = error{
    ChannelClosed,
    Timeout,
    ProtocolError,
    NotConnected,
    EndOfStream,
    PublishNacked,
    BrokenPipe,
} || Allocator.Error;

pub const UriError = error{
    InvalidUri,
} || Allocator.Error;

pub const ConnectionError = ConnectError || HandshakeError || ChannelOpenError || ChannelError || UriError;

pub const Endpoint = struct {
    host: []const u8,
    port: u16,
};

/// Strategy for resolving the list of endpoints to connect to.
/// Modeled after the Java client's AddressResolver interface.
pub const AddressResolver = union(enum) {
    /// Default: use host/port and hosts from ConnectionOptions directly
    default,
    /// Custom resolver function that returns endpoints to try, in order.
    /// The returned slice must remain valid until the next call or connection close.
    custom: *const fn () anyerror![]const Endpoint,
};

pub const ConnectionOptions = struct {
    host: []const u8 = "localhost",
    port: u16 = constants.default_port,
    /// Additional endpoints for redundancy. Tried after the primary host:port.
    hosts: []const Endpoint = &.{},
    username: []const u8 = "guest",
    password: []const u8 = "guest",
    virtual_host: []const u8 = "/",
    heartbeat: u16 = constants.default_heartbeat,
    channel_max: u16 = constants.default_channel_max,
    frame_max: u32 = constants.default_frame_max,
    connection_name: ?[]const u8 = null,
    /// TCP connection timeout in milliseconds, 0 means no timeout
    connection_timeout_ms: u32 = 15_000,
    /// Maximum number of publisher confirm promises to cache per channel
    confirm_promise_pool_size: u16 = 64,
    /// Timeout in seconds for RPC continuations (channel.open-ok,
    /// queue.declare-ok, connection.close-ok, etc.)
    continuation_timeout_s: u16 = 5,
    /// SASL mechanism: "PLAIN" (default) or "EXTERNAL" (x509 certificate auth)
    auth_mechanism: []const u8 = "PLAIN",
    tls: ?TlsOptions = null,
    recovery: recovery_mod.RecoveryConfig = .{},
    address_resolver: AddressResolver = .default,

    /// Parse an AMQP URI into ConnectionOptions.
    /// Supports percent-encoded usernames, passwords, and vhosts.
    pub fn fromUri(allocator: Allocator, uri_string: []const u8) !ConnectionOptions {
        var opts = ConnectionOptions{};

        const uri = std.Uri.parse(uri_string) catch return error.InvalidUri;

        if (!std.mem.eql(u8, uri.scheme, "amqp") and !std.mem.eql(u8, uri.scheme, "amqps")) {
            return error.InvalidUri;
        }

        if (uri.host) |host| {
            opts.host = try host.toRawMaybeAlloc(allocator);
        }

        if (uri.port) |port| {
            opts.port = port;
        } else if (std.mem.eql(u8, uri.scheme, "amqps")) {
            opts.port = constants.default_tls_port;
        }

        if (uri.user) |user| {
            opts.username = try user.toRawMaybeAlloc(allocator);
        }

        if (uri.password) |pw| {
            opts.password = try pw.toRawMaybeAlloc(allocator);
        }

        // AMQP 0-9-1 virtual host is the path without the leading '/'.
        // Empty path or "/" means default vhost "/".
        // "/%2F" also means default vhost "/".
        const raw_path = switch (uri.path) {
            .raw => |r| r,
            .percent_encoded => |p| p,
        };
        if (raw_path.len > 1) {
            const encoded = raw_path[1..];
            const buf = try allocator.alloc(u8, encoded.len);
            const decoded = std.Uri.percentDecodeBackwards(buf, encoded);
            if (decoded.ptr == buf.ptr and decoded.len == buf.len) {
                opts.virtual_host = decoded;
            } else {
                // percentDecodeBackwards returned a subslice: copy and free
                opts.virtual_host = try allocator.dupe(u8, decoded);
                allocator.free(buf);
            }
        }

        return opts;
    }
};

/// An AMQP 0-9-1 connection to a RabbitMQ server.
pub const Connection = struct {
    allocator: Allocator,
    transport: Transport,
    options: ConnectionOptions,

    // Negotiated values
    negotiated_channel_max: u16 = 0,
    negotiated_frame_max: u32 = 0,
    negotiated_heartbeat: u16 = 0,
    server_properties: FieldTable = FieldTable.empty,

    // Channel management
    channels: std.AutoHashMap(u16, *Channel),
    next_channel_id: u16 = 1,
    channel_mutex: Mutex = .init,

    // Connection state
    is_open: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    reader_thread: ?Thread = null,
    heartbeat_thread: ?Thread = null,
    reader_exit: Io.Event = .unset,
    heartbeat_exit: Io.Event = .unset,
    should_stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    // Monotonic nanosecond timestamp of the last frame received, for missed heartbeat detection
    last_frame_at: std.atomic.Value(i64) = std.atomic.Value(i64).init(0),
    write_mutex: Mutex = .init,

    // Waiting for RPC responses on channel 0
    rpc_mutex: Mutex = .init,
    rpc_signal: Notify = .init,
    rpc_response: ?Method = null,

    // Event callbacks (legacy)
    on_blocked: ?*const fn (reason: []const u8) void = null,
    on_unblocked: ?*const fn () void = null,
    on_close: ?*const fn (code: u16, text: []const u8) void = null,

    // Event listener system
    event_listeners: events.EventListeners(ConnectionEvent) = .{},

    // Topology tracking for recovery
    topology: recovery_mod.TopologyRegistry = undefined,

    // Blocked state
    blocked_reason: ?[]const u8 = null,

    /// Open a new connection with the given options. Resolves endpoints via the
    /// address resolver, then tries each in order until one succeeds.
    pub fn open(allocator: Allocator, options: ConnectionOptions) !*Connection {
        var transport = try resolveAndConnect(allocator, options);
        errdefer transport.close();

        const conn = try allocator.create(Connection);
        errdefer allocator.destroy(conn);

        conn.* = .{
            .allocator = allocator,
            .transport = transport,
            .options = options,
            .channels = std.AutoHashMap(u16, *Channel).init(allocator),
            .topology = recovery_mod.TopologyRegistry.init(allocator),
        };

        try conn.performHandshake();
        conn.is_open.store(true, .release);
        conn.last_frame_at.store(@intCast(Io.Clock.awake.now(getIo()).nanoseconds), .release);

        // Start reader thread
        conn.reader_thread = try Thread.spawn(.{}, readerLoop, .{conn});

        // Start heartbeat thread if negotiated
        if (conn.negotiated_heartbeat > 0) {
            conn.heartbeat_thread = try Thread.spawn(.{}, heartbeatLoop, .{conn});
        }

        return conn;
    }

    /// Open a connection from an AMQP URI.
    pub fn openUri(allocator: Allocator, uri: []const u8) !*Connection {
        const options = try ConnectionOptions.fromUri(allocator, uri);
        return open(allocator, options);
    }

    /// Close the connection gracefully.
    pub fn close(self: *Connection) void {
        // Atomic swap to ensure only one thread executes the close sequence
        if (self.is_open.cmpxchgStrong(true, false, .acq_rel, .monotonic) != null) return;

        // Close all channels internally
        self.channel_mutex.lockUncancelable(getIo());
        var it = self.channels.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.closeInternal();
        }
        self.channel_mutex.unlock(getIo());

        // Send connection.close and wait briefly for close-ok
        self.write_mutex.lockUncancelable(getIo());
        self.transport.sendMethod(0, .{ .connection_close = .{
            .reply_code = 200,
            .reply_text = "Normal shutdown",
            .class_id = 0,
            .method_id = 0,
        } }) catch {};
        self.write_mutex.unlock(getIo());

        _ = self.waitForRpc(self.continuationTimeoutNs()) catch {};

        self.shutdown();
    }

    /// Open a new channel on this connection.
    pub fn openChannel(self: *Connection) !*Channel {
        if (!self.is_open.load(.acquire)) return error.NotConnected;

        self.channel_mutex.lockUncancelable(getIo());
        const channel_id = self.next_channel_id;
        if (channel_id > self.negotiated_channel_max) {
            self.channel_mutex.unlock(getIo());
            return error.ChannelLimitReached;
        }
        self.next_channel_id += 1;
        self.channel_mutex.unlock(getIo());

        const ch = try Channel.init(self.allocator, self, channel_id);
        errdefer ch.deinit();

        // Add to map before sending so the reader thread can dispatch the response
        self.channel_mutex.lockUncancelable(getIo());
        try self.channels.put(channel_id, ch);
        self.channel_mutex.unlock(getIo());

        self.write_mutex.lockUncancelable(getIo());
        self.transport.sendMethod(channel_id, .{ .channel_open = .{} }) catch |err| {
            self.write_mutex.unlock(getIo());
            self.channel_mutex.lockUncancelable(getIo());
            _ = self.channels.remove(channel_id);
            self.channel_mutex.unlock(getIo());
            return err;
        };
        self.write_mutex.unlock(getIo());

        const response = try ch.awaitMethod();
        switch (response) {
            .channel_open_ok => {},
            .channel_close => |cc| {
                log.err("channel open refused: {s}", .{cc.reply_text});
                self.channel_mutex.lockUncancelable(getIo());
                _ = self.channels.remove(channel_id);
                self.channel_mutex.unlock(getIo());
                return error.ChannelClosed;
            },
            else => {
                self.channel_mutex.lockUncancelable(getIo());
                _ = self.channels.remove(channel_id);
                self.channel_mutex.unlock(getIo());
                return error.ProtocolError;
            },
        }

        self.topology.recordChannel(self.allocator, .{ .id = channel_id }) catch {};

        return ch;
    }

    //
    // Internal
    //

    fn performHandshake(self: *Connection) !void {
        try self.transport.sendProtocolHeader();

        // Read connection.start
        const start_frame = try self.transport.readFrame(self.allocator);
        const start = switch (start_frame) {
            .method => |mf| switch (mf.method) {
                .connection_start => |s| s,
                else => return error.ProtocolError,
            },
            else => return error.ProtocolError,
        };
        self.server_properties = start.server_properties;

        // Build client properties
        var props_list: std.ArrayList(FieldTable.Entry) = .empty;
        defer props_list.deinit(self.allocator);
        try props_list.append(self.allocator, .{ .key = "product", .value = .{ .long_string = constants.product } });
        try props_list.append(self.allocator, .{ .key = "version", .value = .{ .long_string = constants.version } });
        try props_list.append(self.allocator, .{ .key = "platform", .value = .{ .long_string = constants.platform } });
        try props_list.append(self.allocator, .{ .key = "capabilities", .value = .{ .table = try self.buildCapabilities() } });
        if (self.options.connection_name) |name| {
            try props_list.append(self.allocator, .{ .key = "connection_name", .value = .{ .long_string = name } });
        }
        var client_props = FieldTable{
            .entries = try props_list.toOwnedSlice(self.allocator),
            .allocator = self.allocator,
        };
        defer client_props.deinit();

        // Build auth response based on mechanism
        const mechanism = self.options.auth_mechanism;
        var auth_buf: []u8 = &.{};
        defer if (auth_buf.len > 0) self.allocator.free(auth_buf);

        if (std.mem.eql(u8, mechanism, "PLAIN")) {
            const auth_len = 1 + self.options.username.len + 1 + self.options.password.len;
            auth_buf = try self.allocator.alloc(u8, auth_len);
            auth_buf[0] = 0;
            @memcpy(auth_buf[1..][0..self.options.username.len], self.options.username);
            auth_buf[1 + self.options.username.len] = 0;
            @memcpy(auth_buf[2 + self.options.username.len ..], self.options.password);
        }
        // EXTERNAL uses an empty response (identity from the TLS certificate)

        try self.transport.sendMethod(0, .{ .connection_start_ok = .{
            .client_properties = client_props,
            .mechanism = mechanism,
            .response = auth_buf,
            .locale = "en_US",
        } });

        // The server may send connection.secure (SASL challenge) before tune
        var tune: protocol.method.ConnectionTune = undefined;
        while (true) {
            const frame = try self.transport.readFrame(self.allocator);
            switch (frame) {
                .method => |mf| switch (mf.method) {
                    .connection_tune => |t| {
                        tune = t;
                        break;
                    },
                    .connection_secure => |sec| {
                        // Respond with the same auth credentials
                        try self.transport.sendMethod(0, .{ .connection_secure_ok = .{
                            .response = sec.challenge,
                        } });
                    },
                    .connection_close => |cc| {
                        log.err("connection refused: {s}", .{cc.reply_text});
                        if (cc.reply_code == 403) return error.AuthenticationFailed;
                        return error.ConnectionClosed;
                    },
                    else => return error.ProtocolError,
                },
                else => return error.ProtocolError,
            }
        }

        // Negotiate values
        self.negotiated_channel_max = if (tune.channel_max == 0) self.options.channel_max else @min(tune.channel_max, self.options.channel_max);
        self.negotiated_frame_max = if (tune.frame_max == 0) self.options.frame_max else @min(tune.frame_max, self.options.frame_max);
        self.negotiated_heartbeat = if (self.options.heartbeat == 0) tune.heartbeat else @min(tune.heartbeat, self.options.heartbeat);

        // Send connection.tune-ok
        try self.transport.sendMethod(0, .{ .connection_tune_ok = .{
            .channel_max = self.negotiated_channel_max,
            .frame_max = self.negotiated_frame_max,
            .heartbeat = self.negotiated_heartbeat,
        } });

        // Send connection.open
        try self.transport.sendMethod(0, .{ .connection_open = .{
            .virtual_host = self.options.virtual_host,
        } });

        // Read connection.open-ok
        const open_frame = try self.transport.readFrame(self.allocator);
        switch (open_frame) {
            .method => |mf| switch (mf.method) {
                .connection_open_ok => {},
                .connection_close => |cc| {
                    log.err("connection open refused: {s}", .{cc.reply_text});
                    return error.ConnectionClosed;
                },
                else => return error.ProtocolError,
            },
            else => return error.ProtocolError,
        }
    }

    fn buildCapabilities(self: *Connection) !FieldTable {
        const cap_entries = [_]FieldTable.Entry{
            .{ .key = "authentication_failure_close", .value = .{ .boolean = true } },
            .{ .key = "basic.nack", .value = .{ .boolean = true } },
            .{ .key = "connection.blocked", .value = .{ .boolean = true } },
            .{ .key = "consumer_cancel_notify", .value = .{ .boolean = true } },
            .{ .key = "exchange_exchange_bindings", .value = .{ .boolean = true } },
            .{ .key = "publisher_confirms", .value = .{ .boolean = true } },
        };
        return FieldTable.fromEntries(self.allocator, &cap_entries);
    }

    /// The background reader loop dispatches frames to channels.
    fn readerLoop(self: *Connection) void {
        defer self.reader_exit.set(getIo());
        while (!self.should_stop.load(.acquire)) {
            const frame = self.transport.readFrame(self.allocator) catch |err| {
                if (self.should_stop.load(.acquire)) break;
                log.warn("reader loop error: {}", .{err});

                if (self.options.recovery.enabled) {
                    self.attemptRecovery();
                    if (self.is_open.load(.acquire)) continue;
                }
                break;
            };

            self.last_frame_at.store(@intCast(Io.Clock.awake.now(getIo()).nanoseconds), .release);
            self.dispatchFrame(frame);
        }

        self.is_open.store(false, .release);
    }

    fn attemptRecovery(self: *Connection) void {
        const config = self.options.recovery;
        self.is_open.store(false, .release);
        self.event_listeners.emit(.{ .recovery_started = {} });

        // Stop heartbeat thread
        if (self.heartbeat_thread) |t| {
            self.should_stop.store(true, .release);
            t.join();
            self.heartbeat_thread = null;
            self.should_stop.store(false, .release);
        }

        // Close all channels internally
        self.channel_mutex.lockUncancelable(getIo());
        var ch_it = self.channels.iterator();
        while (ch_it.next()) |entry| {
            entry.value_ptr.*.closeInternal();
        }
        self.channel_mutex.unlock(getIo());

        // Close the failed transport once before the retry loop
        self.transport.close();

        var attempt: u32 = 0;
        while (config.max_attempts == null or attempt < config.max_attempts.?) {
            const backoff_ms = recovery_mod.nextBackoff(attempt, config);
            log.info("recovery attempt {d}, waiting {d}ms", .{ attempt + 1, backoff_ms });
            getIo().sleep(.{ .nanoseconds = @intCast(backoff_ms * std.time.ns_per_ms) }, .boot) catch {};

            const transport = resolveAndConnect(self.allocator, self.options) catch |err| {
                log.warn("recovery attempt {d} failed: {}", .{ attempt + 1, err });
                attempt += 1;
                continue;
            };

            self.transport = transport;
            self.performHandshake() catch |err| {
                log.warn("handshake failed during recovery: {}", .{err});
                self.transport.close();
                attempt += 1;
                continue;
            };

            self.recoverChannelsAndTopology();

            self.is_open.store(true, .release);
            self.last_frame_at.store(@intCast(Io.Clock.awake.now(getIo()).nanoseconds), .release);
            self.event_listeners.emit(.{ .recovery_succeeded = {} });

            if (self.negotiated_heartbeat > 0) {
                self.heartbeat_exit = .unset;
                self.heartbeat_thread = Thread.spawn(.{}, heartbeatLoop, .{self}) catch null;
            }

            log.info("connection recovered after {d} attempt(s)", .{attempt + 1});
            return;
        }

        self.event_listeners.emit(.{ .recovery_failed = "max attempts exhausted" });
        if (config.max_attempts) |n| {
            log.err("recovery failed after {d} attempt(s)", .{n});
        }
    }

    fn recoverChannelsAndTopology(self: *Connection) void {
        // Re-open each channel that was open before the failure
        for (self.topology.channels.items) |recorded_ch| {
            self.transport.sendMethod(recorded_ch.id, .{ .channel_open = .{} }) catch {
                log.warn("failed to re-open channel {d}", .{recorded_ch.id});
                continue;
            };
            // Consume channel.open-ok synchronously
            _ = self.transport.readFrame(self.allocator) catch continue;

            if (recorded_ch.prefetch_count > 0) {
                self.transport.sendMethod(recorded_ch.id, .{ .basic_qos = .{
                    .prefetch_count = recorded_ch.prefetch_count,
                    .global = recorded_ch.prefetch_global,
                } }) catch {};
                _ = self.transport.readFrame(self.allocator) catch {};
            }

            if (recorded_ch.confirm_mode) {
                self.transport.sendMethod(recorded_ch.id, .{ .confirm_select = .{} }) catch {};
                _ = self.transport.readFrame(self.allocator) catch {};
            }

            self.channel_mutex.lockUncancelable(getIo());
            if (self.channels.get(recorded_ch.id)) |ch| {
                ch.is_open.store(true, .release);
                ch.rpc_mutex.lockUncancelable(getIo());
                ch.rpc_response = null;
                ch.rpc_mutex.unlock(getIo());
                ch.event_listeners.emit(.{ .recovered = {} });
            }
            self.channel_mutex.unlock(getIo());
        }

        // Replay topology entries, respecting the filter
        for (self.topology.entries.items, 0..) |entry, i| {
            if (!self.topology.filter.shouldRecover(entry)) continue;

            switch (entry) {
                .exchange => |ex| {
                    self.transport.sendMethod(ex.channel_id, .{ .exchange_declare = .{
                        .exchange = ex.name,
                        .exchange_type = ex.exchange_type,
                        .durable = ex.durable,
                        .auto_delete = ex.auto_delete,
                        .internal = ex.internal,
                        .arguments = ex.arguments,
                    } }) catch {};
                    _ = self.transport.readFrame(self.allocator) catch {};
                },
                .queue => |q| {
                    if (q.exclusive) continue;
                    // For server-named queues, declare with empty name to get a new one
                    const declare_name = if (q.server_named) "" else q.name;
                    self.transport.sendMethod(q.channel_id, .{ .queue_declare = .{
                        .queue = declare_name,
                        .durable = q.durable,
                        .exclusive = false,
                        .auto_delete = q.auto_delete,
                        .arguments = q.arguments,
                    } }) catch {};

                    // Read queue.declare-ok to handle server-named queue renames
                    const qframe = self.transport.readFrame(self.allocator) catch continue;
                    if (q.server_named) {
                        switch (qframe) {
                            .method => |mf| switch (mf.method) {
                                .queue_declare_ok => |ok| {
                                    if (!std.mem.eql(u8, ok.queue, q.name)) {
                                        self.event_listeners.emit(.{ .recovery_queue_name_changed = .{
                                            .old_name = q.name,
                                            .new_name = ok.queue,
                                        } });
                                        self.topology.updateQueueName(self.allocator, q.name, ok.queue) catch |err| {
                                            log.err("failed to update queue name mapping: {}", .{err});
                                        };
                                        self.topology.entries.items[i].queue.name = ok.queue;
                                    }
                                },
                                else => {},
                            },
                            else => {},
                        }
                    }
                },
                .queue_binding => |b| {
                    const dest = self.topology.resolveQueueName(b.destination);
                    self.transport.sendMethod(b.channel_id, .{ .queue_bind = .{
                        .queue = dest,
                        .exchange = b.source,
                        .routing_key = b.routing_key,
                        .arguments = b.arguments,
                    } }) catch {};
                    _ = self.transport.readFrame(self.allocator) catch {};
                },
                .exchange_binding => |b| {
                    self.transport.sendMethod(b.channel_id, .{ .exchange_bind = .{
                        .destination = b.destination,
                        .source = b.source,
                        .routing_key = b.routing_key,
                        .arguments = b.arguments,
                    } }) catch {};
                    _ = self.transport.readFrame(self.allocator) catch {};
                },
                .consumer => |c| {
                    const resolved_queue = self.topology.resolveQueueName(c.queue);
                    self.transport.sendMethod(c.channel_id, .{ .basic_consume = .{
                        .queue = resolved_queue,
                        .consumer_tag = c.consumer_tag,
                        .no_ack = c.no_ack,
                        .exclusive = c.exclusive,
                    } }) catch {};
                    _ = self.transport.readFrame(self.allocator) catch {};
                },
            }
        }
    }

    /// Whether the connection is open.
    pub fn isOpen(self: *Connection) bool {
        return self.is_open.load(.acquire);
    }

    /// Whether the connection is currently blocked by the server.
    pub fn isBlocked(self: *Connection) bool {
        return self.blocked_reason != null;
    }

    /// The reason the connection was blocked, or null if not blocked.
    pub fn blockedReason(self: *Connection) ?[]const u8 {
        return self.blocked_reason;
    }

    /// Set the topology recovery filter.
    pub fn setTopologyRecoveryFilter(self: *Connection, filter: recovery_mod.TopologyRecoveryFilter) void {
        self.topology.setFilter(filter);
    }

    fn dispatchFrame(self: *Connection, frame: frame_mod.Frame) void {
        const ch_id = frame.channel();

        if (ch_id == 0) {
            // Connection-level frame
            switch (frame) {
                .method => |mf| self.handleConnectionMethod(mf.method),
                .heartbeat => {}, // Heartbeat received, nothing to do
                else => log.warn("unexpected frame type on channel 0", .{}),
            }
            return;
        }

        // Dispatch to channel
        self.channel_mutex.lockUncancelable(getIo());
        const channel = self.channels.get(ch_id);
        self.channel_mutex.unlock(getIo());

        if (channel) |ch| {
            ch.handleFrame(frame);
        } else {
            log.warn("frame for unknown channel {d}", .{ch_id});
        }
    }

    fn handleConnectionMethod(self: *Connection, m: Method) void {
        switch (m) {
            .connection_close => |cc| {
                self.write_mutex.lockUncancelable(getIo());
                self.transport.sendMethod(0, .{ .connection_close_ok = {} }) catch {};
                self.write_mutex.unlock(getIo());

                if (self.on_close) |cb| cb(cc.reply_code, cc.reply_text);
                self.event_listeners.emit(.{ .closed = .{ .code = cc.reply_code, .text = cc.reply_text } });
                self.is_open.store(false, .release);
            },
            .connection_close_ok, .connection_update_secret_ok => {
                self.rpc_mutex.lockUncancelable(getIo());
                self.rpc_response = m;
                self.rpc_signal.signal(getIo());
                self.rpc_mutex.unlock(getIo());
            },
            .connection_blocked => |b| {
                self.blocked_reason = b.reason;
                if (self.on_blocked) |cb| cb(b.reason);
                self.event_listeners.emit(.{ .blocked = b.reason });
            },
            .connection_unblocked => {
                self.blocked_reason = null;
                if (self.on_unblocked) |cb| cb();
                self.event_listeners.emit(.{ .unblocked = {} });
            },
            else => log.warn("unexpected connection method", .{}),
        }
    }

    fn heartbeatLoop(self: *Connection) void {
        defer self.heartbeat_exit.set(getIo());
        const interval_ns: u64 = @as(u64, self.negotiated_heartbeat) * std.time.ns_per_s / 2;
        const deadline_ns: u64 = @as(u64, self.negotiated_heartbeat) * std.time.ns_per_s * 2;
        const check_interval: u64 = 500 * std.time.ns_per_ms;
        while (!self.should_stop.load(.acquire)) {
            // Sleep in short intervals so we can exit quickly on shutdown
            var elapsed: u64 = 0;
            while (elapsed < interval_ns) {
                if (self.should_stop.load(.acquire)) return;
                getIo().sleep(.{ .nanoseconds = @intCast(check_interval) }, .boot) catch {};
                elapsed += check_interval;
            }
            if (self.should_stop.load(.acquire)) break;

            // Check for missed heartbeats: no frame received within 2x the interval
            const last = self.last_frame_at.load(.acquire);
            if (last > 0) {
                const now_ns: i64 = @intCast(Io.Clock.awake.now(getIo()).nanoseconds);
                const since: u64 = @intCast(@max(0, now_ns - last));
                if (since > deadline_ns) {
                    log.err("missed heartbeats: no frame received for {d}ms, closing connection", .{since / std.time.ns_per_ms});
                    self.is_open.store(false, .release);
                    self.event_listeners.emit(.{ .closed = .{ .code = 0, .text = "missed heartbeats" } });
                    break;
                }
            }

            self.write_mutex.lockUncancelable(getIo());
            self.transport.sendFrame(.{ .heartbeat = {} }) catch {
                self.write_mutex.unlock(getIo());
                break;
            };
            self.write_mutex.unlock(getIo());
        }
    }

    pub fn continuationTimeoutNs(self: *const Connection) u64 {
        return @as(u64, self.options.continuation_timeout_s) * std.time.ns_per_s;
    }

    fn waitForRpc(self: *Connection, timeout_ns: u64) !Method {
        const io = getIo();
        const start = Io.Timestamp.now(io, .boot);
        const limit: i96 = @intCast(timeout_ns);

        self.rpc_mutex.lockUncancelable(io);
        defer self.rpc_mutex.unlock(io);

        while (self.rpc_response == null) {
            const elapsed = Io.Timestamp.now(io, .boot).nanoseconds - start.nanoseconds;
            if (elapsed >= limit) return error.Timeout;
            const remaining = limit - elapsed;
            const timeout: Io.Timeout = .{ .duration = .{ .raw = .{ .nanoseconds = remaining }, .clock = .awake } };
            const expected = self.rpc_signal.snapshot();
            self.rpc_mutex.unlock(io);
            self.rpc_signal.waitTimeout(io, expected, timeout);
            self.rpc_mutex.lockUncancelable(io);
        }
        const response = self.rpc_response.?;
        self.rpc_response = null;
        return response;
    }

    fn shutdown(self: *Connection) void {
        self.is_open.store(false, .release);
        self.should_stop.store(true, .release);

        // Shut down the socket to unblock the reader thread's blocking readv.
        // This makes readv return 0 (EOF) without closing the fd, avoiding
        // a BADF panic in Zig's IO layer.
        self.transport.shutdownSocket();

        if (self.reader_thread) |t| {
            t.join();
            self.reader_thread = null;
        }
        if (self.heartbeat_thread) |t| {
            t.join();
            self.heartbeat_thread = null;
        }

        self.transport.close();
    }

    pub fn deinit(self: *Connection) void {
        if (self.is_open.load(.acquire)) self.close();

        // Clean up channels
        self.channel_mutex.lockUncancelable(getIo());
        var it = self.channels.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
        }
        self.channels.deinit();
        self.channel_mutex.unlock(getIo());

        self.event_listeners.deinit(self.allocator);
        self.topology.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    /// Register a listener for connection events.
    pub fn addEventListener(self: *Connection, cb: *const fn (ConnectionEvent) void) !void {
        try self.event_listeners.add(self.allocator, cb);
    }

    /// Update the secret (credential rotation). Sends connection.update-secret to the server.
    pub fn updateSecret(self: *Connection, new_secret: []const u8, reason: []const u8) !void {
        try self.sendMethod(0, .{ .connection_update_secret = .{
            .new_secret = new_secret,
            .reason = reason,
        } });

        const response = try self.waitForRpc(15 * std.time.ns_per_s);
        switch (response) {
            .connection_update_secret_ok => {},
            .connection_close => |cc| {
                log.err("update-secret refused: {s}", .{cc.reply_text});
                return error.ConnectionClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Send a method on a channel (thread-safe).
    pub fn sendMethod(self: *Connection, channel_id: u16, m: Method) !void {
        self.write_mutex.lockUncancelable(getIo());
        defer self.write_mutex.unlock(getIo());
        try self.transport.sendMethod(channel_id, m);
    }

    /// Send a method frame without flushing (thread-safe). Caller must call flushWrite.
    pub fn sendMethodNoFlush(self: *Connection, channel_id: u16, m: Method) !void {
        self.write_mutex.lockUncancelable(getIo());
        defer self.write_mutex.unlock(getIo());
        try self.transport.sendFrameNoFlush(.{ .method = .{ .channel = channel_id, .method = m } });
    }

    /// Flush the write buffer to the socket (thread-safe).
    pub fn flushWrite(self: *Connection) !void {
        self.write_mutex.lockUncancelable(getIo());
        defer self.write_mutex.unlock(getIo());
        try self.transport.flush();
    }

    /// Send content frames (header + body) on a channel (thread-safe).
    pub fn sendContent(self: *Connection, channel_id: u16, props: protocol.properties.BasicProperties, body: []const u8) !void {
        self.write_mutex.lockUncancelable(getIo());
        defer self.write_mutex.unlock(getIo());
        try self.transport.sendContent(channel_id, props, body, self.negotiated_frame_max);
    }

    /// Send a complete publish (method + header + body) in one batched write (thread-safe).
    pub fn sendPublish(self: *Connection, channel_id: u16, method: protocol.method.Method, props: protocol.properties.BasicProperties, body: []const u8, do_flush: bool) !void {
        self.write_mutex.lockUncancelable(getIo());
        defer self.write_mutex.unlock(getIo());
        try self.transport.sendPublish(channel_id, method, props, body, self.negotiated_frame_max, do_flush);
    }

    /// Send a batch of publishes (same exchange/routing key, different bodies).
    pub fn sendPublishBatch(
        self: *Connection,
        channel_id: u16,
        method: protocol.method.Method,
        props: protocol.properties.BasicProperties,
        bodies: []const []const u8,
        do_flush: bool,
    ) !void {
        self.write_mutex.lockUncancelable(getIo());
        defer self.write_mutex.unlock(getIo());
        try self.transport.sendPublishBatch(channel_id, method, props, bodies, self.negotiated_frame_max, do_flush);
    }

    /// Resolve endpoints via the configured address resolver and try each
    /// in order until a transport connection succeeds.
    fn resolveAndConnect(allocator: Allocator, options: ConnectionOptions) !Transport {
        switch (options.address_resolver) {
            .custom => |resolver| {
                const endpoints = try resolver();
                var last_err: anyerror = error.ConnectionRefused;
                for (endpoints) |ep| {
                    return connectTransport(allocator, options, ep.host, ep.port) catch |err| {
                        last_err = err;
                        continue;
                    };
                }
                return last_err;
            },
            .default => {
                return connectTransport(allocator, options, options.host, options.port) catch |primary_err| {
                    for (options.hosts) |endpoint| {
                        return connectTransport(allocator, options, endpoint.host, endpoint.port) catch continue;
                    }
                    return primary_err;
                };
            },
        }
    }

    fn connectTransport(allocator: Allocator, options: ConnectionOptions, host: []const u8, port: u16) !Transport {
        // Zig 0.16 Threaded IO does not support connect timeouts yet,
        // so always use .none until the runtime adds support.
        _ = options.connection_timeout_ms;
        const timeout: Io.Timeout = .none;

        if (options.tls) |tls_opts| {
            return Transport.connectTls(allocator, host, port, tls_opts, timeout);
        }
        return Transport.connect(allocator, host, port, timeout);
    }

    /// Remove a channel from the connection's channel map.
    pub fn removeChannel(self: *Connection, channel_id: u16) void {
        self.channel_mutex.lockUncancelable(getIo());
        _ = self.channels.remove(channel_id);
        self.channel_mutex.unlock(getIo());
    }
};

//
// URI parsing tests
//

test "URI: basic amqp://" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqp://localhost");
    try std.testing.expectEqualSlices(u8, "localhost", opts.host);
    try std.testing.expectEqual(5672, opts.port);
    try std.testing.expectEqualSlices(u8, "guest", opts.username);
    try std.testing.expectEqualSlices(u8, "guest", opts.password);
    try std.testing.expectEqualSlices(u8, "/", opts.virtual_host);
}

test "URI: amqps:// defaults to port 5671" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqps://rabbitmq.example.com");
    try std.testing.expectEqualSlices(u8, "rabbitmq.example.com", opts.host);
    try std.testing.expectEqual(5671, opts.port);
}

test "URI: explicit port overrides scheme default" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqps://host:5672");
    try std.testing.expectEqual(5672, opts.port);
}

test "URI: username and password" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqp://myuser:mypass@host");
    try std.testing.expectEqualSlices(u8, "myuser", opts.username);
    try std.testing.expectEqualSlices(u8, "mypass", opts.password);
}

test "URI: percent-encoded username and password" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqp://user%40name:p%23ss@host");
    defer allocator.free(opts.username);
    defer allocator.free(opts.password);
    try std.testing.expectEqualSlices(u8, "user@name", opts.username);
    try std.testing.expectEqualSlices(u8, "p#ss", opts.password);
}

test "URI: vhost from path" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqp://localhost/production");
    defer allocator.free(opts.virtual_host);
    try std.testing.expectEqualSlices(u8, "production", opts.virtual_host);
}

test "URI: percent-encoded vhost" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqp://localhost/%2F");
    defer allocator.free(opts.virtual_host);
    try std.testing.expectEqualSlices(u8, "/", opts.virtual_host);
}

test "URI: empty path means default vhost" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqp://localhost");
    try std.testing.expectEqualSlices(u8, "/", opts.virtual_host);
}

test "URI: slash-only path means default vhost" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqp://localhost/");
    try std.testing.expectEqualSlices(u8, "/", opts.virtual_host);
}

test "URI: full example with all components" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqp://admin:secret@rmq.local:5673/staging");
    defer allocator.free(opts.virtual_host);
    try std.testing.expectEqualSlices(u8, "rmq.local", opts.host);
    try std.testing.expectEqual(5673, opts.port);
    try std.testing.expectEqualSlices(u8, "admin", opts.username);
    try std.testing.expectEqualSlices(u8, "secret", opts.password);
    try std.testing.expectEqualSlices(u8, "staging", opts.virtual_host);
}

test "URI: invalid scheme rejected" {
    const allocator = std.testing.allocator;
    const result = ConnectionOptions.fromUri(allocator, "http://localhost");
    try std.testing.expectError(error.InvalidUri, result);
}

test "URI: vhost with spaces" {
    const allocator = std.testing.allocator;
    const opts = try ConnectionOptions.fromUri(allocator, "amqp://localhost/my%20vhost");
    defer allocator.free(opts.virtual_host);
    try std.testing.expectEqualSlices(u8, "my vhost", opts.virtual_host);
}
