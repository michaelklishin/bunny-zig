/// AMQP 0-9-1 channel: the primary surface for queue, exchange, publish, and consume operations.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Thread = std.Thread;
const Io = std.Io;
const Mutex = Io.Mutex;
const Condition = Io.Condition;

/// Per-message publisher confirm future, modeled after Swift's CheckedContinuation
/// and .NET's TaskCompletionSource.
pub const ConfirmPromise = struct {
    event: Io.Event = .unset,
    result: ConfirmResult = .pending,

    pub const ConfirmResult = enum { pending, acked, nacked };

    /// Block until the broker confirms or nacks this message.
    pub fn wait(self: *ConfirmPromise) ConfirmResult {
        self.event.waitUncancelable(getIo());
        return self.result;
    }

    pub fn isResolved(self: *const ConfirmPromise) bool {
        return self.event.isSet();
    }

    fn reset(self: *ConfirmPromise) void {
        self.event = .unset;
        self.result = .pending;
    }
};

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
const BasicProperties = protocol.properties.BasicProperties;
const Connection = @import("connection.zig").Connection;
const Queue = @import("queue.zig").Queue;
const Exchange = @import("exchange.zig").Exchange;
const recovery_mod = @import("recovery.zig");
const events = @import("events.zig");
const ChannelEvent = events.ChannelEvent;
const ConsumerWorkPool = @import("consumer_work_pool.zig").ConsumerWorkPool;

const log = std.log.scoped(.bunny_channel);

/// A delivered message from the server.
pub const Delivery = struct {
    consumer_tag: []const u8,
    delivery_tag: u64,
    redelivered: bool,
    exchange: []const u8,
    routing_key: []const u8,
    properties: BasicProperties,
    body: []const u8,

    /// Get body as a string (assuming UTF-8).
    pub fn bodyString(self: Delivery) []const u8 {
        return self.body;
    }
};

/// Result of a basic.get operation.
pub const GetResult = struct {
    delivery_tag: u64,
    redelivered: bool,
    exchange: []const u8,
    routing_key: []const u8,
    message_count: u32,
    properties: BasicProperties,
    body: []const u8,
};

/// Result of a queue.declare operation.
pub const QueueInfo = struct {
    name: []const u8,
    message_count: u32,
    consumer_count: u32,
};

/// A returned message (when mandatory/immediate fails).
pub const ReturnedMessage = struct {
    reply_code: u16,
    reply_text: []const u8,
    exchange: []const u8,
    routing_key: []const u8,
    properties: BasicProperties,
    body: []const u8,
};

/// Consumer acknowledgement mode.
pub const AckMode = enum {
    manual,
    automatic,
};

/// Exchange type constants.
pub const ExchangeType = struct {
    pub const direct = "direct";
    pub const fanout = "fanout";
    pub const topic = "topic";
    pub const headers = "headers";
};

/// Queue type constants for x-queue-type argument.
pub const QueueType = struct {
    pub const classic = "classic";
    pub const quorum = "quorum";
    pub const stream = "stream";
    /// Tanzu RabbitMQ delayed message queue
    pub const delayed = "delayed-message";
    /// Tanzu RabbitMQ JMS queue
    pub const jms = "jms";
};

/// Options for queue declaration.
pub const QueueDeclareOptions = struct {
    durable: bool = false,
    exclusive: bool = false,
    auto_delete: bool = false,
    passive: bool = false,
    arguments: FieldTable = FieldTable.empty,

    pub fn durableQueue() QueueDeclareOptions {
        return .{ .durable = true };
    }

    pub fn exclusiveQueue() QueueDeclareOptions {
        return .{ .exclusive = true, .auto_delete = true };
    }
};

/// Options for exchange declaration.
pub const ExchangeDeclareOptions = struct {
    durable: bool = false,
    auto_delete: bool = false,
    internal: bool = false,
    passive: bool = false,
    arguments: FieldTable = FieldTable.empty,

    pub fn durableExchange() ExchangeDeclareOptions {
        return .{ .durable = true };
    }
};

/// Controls when the transport buffer is flushed to the socket.
pub const FlushStrategy = enum {
    /// Flush after every publish (default, lowest latency)
    flush_immediately,
    /// Do not flush: caller must call channel.flush() explicitly
    buffered,
};

/// Options for publishing.
pub const PublishOptions = struct {
    exchange: []const u8 = "",
    routing_key: []const u8 = "",
    mandatory: bool = false,
    properties: BasicProperties = BasicProperties.default,
    flush: FlushStrategy = .flush_immediately,
};

/// An AMQP channel. All queue, exchange, publish, and consume operations happen on a channel.
pub const Channel = struct {
    allocator: Allocator,
    connection: *Connection,
    id: u16,
    is_open: std.atomic.Value(bool) = std.atomic.Value(bool).init(true),

    // RPC synchronization: the reader thread posts responses here
    rpc_mutex: Mutex = .init,
    rpc_signal: Condition = .init,
    rpc_response: ?Method = null,

    // Content assembly state (for multi-frame messages)
    pending_header: ?frame_mod.Frame.HeaderFrame = null,
    pending_body: std.ArrayList(u8) = .empty,
    pending_method: ?Method = null,

    // Content for basic.get-ok (separate from consumer deliveries)
    get_properties: BasicProperties = BasicProperties.default,
    get_body: std.ArrayList(u8) = .empty,
    get_ready: bool = false,

    // Delivery queue for consumers
    delivery_mutex: Mutex = .init,
    delivery_signal: Condition = .init,
    deliveries: std.ArrayList(Delivery) = .empty,
    delivery_head: usize = 0,

    // Returned messages
    returns: std.ArrayList(ReturnedMessage) = .empty,
    on_return: ?*const fn (ReturnedMessage) void = null,
    on_cancel: ?*const fn ([]const u8) void = null,

    // Callback-based consumers: tag -> handler
    consumer_callbacks: std.StringHashMap(*const fn (Delivery) void) = undefined,

    // Channel event listeners
    event_listeners: events.EventListeners(ChannelEvent) = .{},

    // Consumer work pool (optional, for dispatching callback consumers off the reader thread)
    work_pool: ?*ConsumerWorkPool = null,

    // Publisher confirms
    confirm_mode: bool = false,
    confirm_tracking: bool = false,
    outstanding_limit: u32 = 0,
    outstanding_count: u32 = 0,
    next_publish_seq_no: u64 = 0,
    confirm_mutex: Mutex = .init,
    confirm_signal: Condition = .init,
    last_confirmed_seq: u64 = 0,
    confirm_promises: std.AutoHashMap(u64, *ConfirmPromise) = undefined,
    promise_pool: std.ArrayList(*ConfirmPromise) = .empty,

    pub fn init(allocator: Allocator, connection: *Connection, id: u16) !*Channel {
        const ch = try allocator.create(Channel);
        ch.* = .{
            .allocator = allocator,
            .connection = connection,
            .id = id,
            .confirm_promises = std.AutoHashMap(u64, *ConfirmPromise).init(allocator),
            .consumer_callbacks = std.StringHashMap(*const fn (Delivery) void).init(allocator),
        };
        return ch;
    }

    pub fn deinit(self: *Channel) void {
        self.pending_body.deinit(self.allocator);
        self.get_body.deinit(self.allocator);
        self.deliveries.deinit(self.allocator);
        self.returns.deinit(self.allocator);
        self.confirm_promises.deinit();
        for (self.promise_pool.items) |p| self.allocator.destroy(p);
        self.promise_pool.deinit(self.allocator);
        self.consumer_callbacks.deinit();
        self.event_listeners.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    //
    // Queue operations
    //

    /// Declare a queue. Returns queue info (name, message count, consumer count).
    pub fn queueDeclare(self: *Channel, name: []const u8, opts: QueueDeclareOptions) !QueueInfo {
        try self.connection.sendMethod(self.id, .{ .queue_declare = .{
            .queue = name,
            .passive = opts.passive,
            .durable = opts.durable,
            .exclusive = opts.exclusive,
            .auto_delete = opts.auto_delete,
            .arguments = opts.arguments,
        } });

        const response = try self.awaitMethod();
        return switch (response) {
            .queue_declare_ok => |ok| blk: {
                self.connection.topology.recordQueue(self.allocator, .{
                    .name = ok.queue,
                    .durable = opts.durable,
                    .exclusive = opts.exclusive,
                    .auto_delete = opts.auto_delete,
                    .server_named = name.len == 0,
                    .channel_id = self.id,
                    .arguments = opts.arguments,
                }) catch {};
                break :blk .{
                    .name = ok.queue,
                    .message_count = ok.message_count,
                    .consumer_count = ok.consumer_count,
                };
            },
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => error.ProtocolError,
        };
    }

    /// Declare a queue and return a handle for convenient operations.
    pub fn declareQueueHandle(self: *Channel, name: []const u8, opts: QueueDeclareOptions) !Queue {
        const info = try self.queueDeclare(name, opts);
        return .{
            .channel = self,
            .name = info.name,
            .message_count = info.message_count,
            .consumer_count = info.consumer_count,
        };
    }

    /// Declare a durable queue.
    pub fn durableQueue(self: *Channel, name: []const u8) !QueueInfo {
        return self.queueDeclare(name, QueueDeclareOptions.durableQueue());
    }

    /// Declare a quorum queue.
    pub fn quorumQueue(self: *Channel, name: []const u8) !QueueInfo {
        const entries = [_]FieldTable.Entry{
            .{ .key = "x-queue-type", .value = .{ .long_string = QueueType.quorum } },
        };
        var args = try FieldTable.fromEntries(self.allocator, &entries);
        defer args.deinit();
        return self.queueDeclare(name, .{ .durable = true, .arguments = args });
    }

    /// Declare a stream queue.
    pub fn streamQueue(self: *Channel, name: []const u8) !QueueInfo {
        const entries = [_]FieldTable.Entry{
            .{ .key = "x-queue-type", .value = .{ .long_string = QueueType.stream } },
        };
        var args = try FieldTable.fromEntries(self.allocator, &entries);
        defer args.deinit();
        return self.queueDeclare(name, .{ .durable = true, .arguments = args });
    }

    /// Declare a delayed message queue (Tanzu RabbitMQ).
    /// When opts.arguments is empty, x-queue-type is set automatically.
    /// When providing custom arguments via QueueArguments, include
    /// .queueType(allocator, "delayed-message") in the builder.
    pub fn delayedQueue(self: *Channel, name: []const u8, opts: QueueDeclareOptions) !QueueInfo {
        const entries = [_]FieldTable.Entry{
            .{ .key = "x-queue-type", .value = .{ .long_string = QueueType.delayed } },
        };
        var type_args = try FieldTable.fromEntries(self.allocator, &entries);
        defer type_args.deinit();
        var merged_opts = opts;
        if (opts.arguments.entries.len == 0) {
            merged_opts.arguments = type_args;
        }
        merged_opts.durable = true;
        return self.queueDeclare(name, merged_opts);
    }

    /// Declare a JMS queue (Tanzu RabbitMQ).
    /// Use QueueArguments for Tanzu-specific arguments like selectorFields.
    pub fn jmsQueue(self: *Channel, name: []const u8, opts: QueueDeclareOptions) !QueueInfo {
        const entries = [_]FieldTable.Entry{
            .{ .key = "x-queue-type", .value = .{ .long_string = QueueType.jms } },
        };
        var type_args = try FieldTable.fromEntries(self.allocator, &entries);
        defer type_args.deinit();
        var merged_opts = opts;
        if (opts.arguments.entries.len == 0) {
            merged_opts.arguments = type_args;
        }
        merged_opts.durable = true;
        return self.queueDeclare(name, merged_opts);
    }

    /// Start consuming with a JMS selector (Tanzu RabbitMQ).
    pub fn basicConsumeJms(self: *Channel, queue_name: []const u8, consumer_tag: []const u8, ack_mode: AckMode, jms_selector: []const u8) ![]const u8 {
        const entries = [_]FieldTable.Entry{
            .{ .key = "x-jms-selector", .value = .{ .long_string = jms_selector } },
        };
        var args = try FieldTable.fromEntries(self.allocator, &entries);
        defer args.deinit();
        return self.basicConsumeWithArgs(queue_name, consumer_tag, ack_mode, false, args);
    }

    /// Declare a temporary (exclusive, auto-delete) queue with a server-generated name.
    pub fn temporaryQueue(self: *Channel) !QueueInfo {
        return self.queueDeclare("", QueueDeclareOptions.exclusiveQueue());
    }

    /// Bind a queue to an exchange.
    pub fn queueBind(self: *Channel, queue: []const u8, exchange: []const u8, routing_key: []const u8) !void {
        return self.queueBindWithArgs(queue, exchange, routing_key, FieldTable.empty);
    }

    /// Bind a queue to an exchange with arguments.
    pub fn queueBindWithArgs(self: *Channel, queue: []const u8, exchange: []const u8, routing_key: []const u8, arguments: FieldTable) !void {
        try self.connection.sendMethod(self.id, .{ .queue_bind = .{
            .queue = queue,
            .exchange = exchange,
            .routing_key = routing_key,
            .arguments = arguments,
        } });
        const response = try self.awaitMethod();
        switch (response) {
            .queue_bind_ok => {
                self.connection.topology.recordQueueBinding(self.allocator, .{
                    .source = exchange,
                    .destination = queue,
                    .routing_key = routing_key,
                    .channel_id = self.id,
                    .arguments = arguments,
                }) catch {};
            },
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Unbind a queue from an exchange.
    pub fn queueUnbind(self: *Channel, queue: []const u8, exchange: []const u8, routing_key: []const u8) !void {
        try self.connection.sendMethod(self.id, .{ .queue_unbind = .{
            .queue = queue,
            .exchange = exchange,
            .routing_key = routing_key,
        } });
        const response = try self.awaitMethod();
        switch (response) {
            .queue_unbind_ok => {},
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Purge a queue. Returns the number of messages purged.
    pub fn queuePurge(self: *Channel, queue: []const u8) !u32 {
        try self.connection.sendMethod(self.id, .{ .queue_purge = .{ .queue = queue } });
        const response = try self.awaitMethod();
        return switch (response) {
            .queue_purge_ok => |ok| ok.message_count,
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => error.ProtocolError,
        };
    }

    /// Delete a queue. Returns the number of messages deleted.
    pub fn queueDelete(self: *Channel, queue: []const u8) !u32 {
        return self.queueDeleteWithOptions(queue, false, false);
    }

    pub fn queueDeleteWithOptions(self: *Channel, queue: []const u8, if_unused: bool, if_empty: bool) !u32 {
        try self.connection.sendMethod(self.id, .{ .queue_delete = .{
            .queue = queue,
            .if_unused = if_unused,
            .if_empty = if_empty,
        } });
        const response = try self.awaitMethod();
        return switch (response) {
            .queue_delete_ok => |ok| ok.message_count,
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => error.ProtocolError,
        };
    }

    //
    // Exchange operations
    //

    /// Declare an exchange.
    pub fn exchangeDeclare(self: *Channel, name: []const u8, exchange_type: []const u8, opts: ExchangeDeclareOptions) !void {
        try self.connection.sendMethod(self.id, .{ .exchange_declare = .{
            .exchange = name,
            .exchange_type = exchange_type,
            .passive = opts.passive,
            .durable = opts.durable,
            .auto_delete = opts.auto_delete,
            .internal = opts.internal,
            .arguments = opts.arguments,
        } });
        const response = try self.awaitMethod();
        switch (response) {
            .exchange_declare_ok => {
                self.connection.topology.recordExchange(self.allocator, .{
                    .name = name,
                    .exchange_type = exchange_type,
                    .durable = opts.durable,
                    .auto_delete = opts.auto_delete,
                    .internal = opts.internal,
                    .channel_id = self.id,
                    .arguments = opts.arguments,
                }) catch {};
            },
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Declare a direct exchange.
    pub fn declareDirect(self: *Channel, name: []const u8) !void {
        return self.exchangeDeclare(name, ExchangeType.direct, ExchangeDeclareOptions.durableExchange());
    }

    /// Declare a fanout exchange.
    pub fn declareFanout(self: *Channel, name: []const u8) !void {
        return self.exchangeDeclare(name, ExchangeType.fanout, ExchangeDeclareOptions.durableExchange());
    }

    /// Declare a topic exchange.
    pub fn declareTopic(self: *Channel, name: []const u8) !void {
        return self.exchangeDeclare(name, ExchangeType.topic, ExchangeDeclareOptions.durableExchange());
    }

    /// Declare a headers exchange.
    pub fn declareHeaders(self: *Channel, name: []const u8) !void {
        return self.exchangeDeclare(name, ExchangeType.headers, ExchangeDeclareOptions.durableExchange());
    }

    /// Declare an exchange and return a handle for convenient operations.
    pub fn declareExchangeHandle(self: *Channel, name: []const u8, exchange_type: []const u8, opts: ExchangeDeclareOptions) !Exchange {
        try self.exchangeDeclare(name, exchange_type, opts);
        return .{ .channel = self, .name = name };
    }

    /// Delete an exchange.
    pub fn exchangeDelete(self: *Channel, name: []const u8) !void {
        return self.exchangeDeleteWithOptions(name, false);
    }

    pub fn exchangeDeleteWithOptions(self: *Channel, name: []const u8, if_unused: bool) !void {
        try self.connection.sendMethod(self.id, .{ .exchange_delete = .{
            .exchange = name,
            .if_unused = if_unused,
        } });
        const response = try self.awaitMethod();
        switch (response) {
            .exchange_delete_ok => {},
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Bind an exchange to another exchange (RabbitMQ extension).
    pub fn exchangeBind(self: *Channel, destination: []const u8, source: []const u8, routing_key: []const u8) !void {
        try self.connection.sendMethod(self.id, .{ .exchange_bind = .{
            .destination = destination,
            .source = source,
            .routing_key = routing_key,
        } });
        const response = try self.awaitMethod();
        switch (response) {
            .exchange_bind_ok => {
                self.connection.topology.recordExchangeBinding(self.allocator, .{
                    .source = source,
                    .destination = destination,
                    .routing_key = routing_key,
                    .channel_id = self.id,
                }) catch {};
            },
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Unbind an exchange from another exchange.
    pub fn exchangeUnbind(self: *Channel, destination: []const u8, source: []const u8, routing_key: []const u8) !void {
        try self.connection.sendMethod(self.id, .{ .exchange_unbind = .{
            .destination = destination,
            .source = source,
            .routing_key = routing_key,
        } });
        const response = try self.awaitMethod();
        switch (response) {
            .exchange_unbind_ok => {},
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    //
    // Publishing
    //

    /// Publish a message. In tracking mode, blocks until the broker confirms.
    pub fn publish(self: *Channel, body: []const u8, opts: PublishOptions) !void {
        const promise = try self.publishAsync(body, opts);
        if (promise) |p| {
            defer self.releasePromise(p);
            const result = p.wait();
            if (result == .nacked) return error.PublishNacked;
        }
    }

    /// Publish a message and return a ConfirmPromise without blocking.
    /// Returns null if confirm mode is not active or tracking is disabled.
    /// The caller must call releasePromise when done with the promise.
    pub fn publishAsync(self: *Channel, body: []const u8, opts: PublishOptions) !?*ConfirmPromise {
        var promise: ?*ConfirmPromise = null;

        if (self.confirm_mode) {
            const io = getIo();
            self.confirm_mutex.lockUncancelable(io);

            // Backpressure: wait if outstanding count is at the limit
            if (self.confirm_tracking and self.outstanding_limit > 0) {
                while (self.outstanding_count >= self.outstanding_limit) {
                    if (!self.is_open.load(.acquire)) {
                        self.confirm_mutex.unlock(io);
                        return error.ChannelClosed;
                    }
                    self.confirm_signal.waitUncancelable(io, &self.confirm_mutex);
                }
            }

            self.next_publish_seq_no += 1;
            const seq = self.next_publish_seq_no;

            if (self.confirm_tracking) {
                self.outstanding_count += 1;
                promise = try self.acquirePromise();
                self.confirm_promises.put(seq, promise.?) catch {};
            }
            self.confirm_mutex.unlock(io);
        }

        try self.connection.sendPublish(self.id, .{ .basic_publish = .{
            .exchange = opts.exchange,
            .routing_key = opts.routing_key,
            .mandatory = opts.mandatory,
        } }, opts.properties, body, opts.flush == .flush_immediately);

        return promise;
    }

    /// Return a promise to the pool for reuse.
    pub fn releasePromise(self: *Channel, p: *ConfirmPromise) void {
        p.reset();
        if (self.promise_pool.items.len < self.connection.options.confirm_promise_pool_size) {
            self.promise_pool.append(self.allocator, p) catch {
                self.allocator.destroy(p);
            };
        } else {
            self.allocator.destroy(p);
        }
    }

    fn acquirePromise(self: *Channel) !*ConfirmPromise {
        if (self.promise_pool.pop()) |p| {
            p.reset();
            return p;
        }
        const p = try self.allocator.create(ConfirmPromise);
        p.* = .{};
        return p;
    }

    /// Publish a message to the default exchange with the queue name as routing key.
    pub fn publishToQueue(self: *Channel, queue: []const u8, body: []const u8, props: BasicProperties) !void {
        return self.publish(body, .{
            .routing_key = queue,
            .properties = props,
        });
    }

    /// Flush the connection's write buffer to the socket.
    /// Only needed when using FlushStrategy.buffered.
    pub fn flushTransport(self: *Channel) !void {
        try self.connection.flushWrite();
    }

    //
    // Consuming
    //

    /// Set QoS prefetch count.
    pub fn basicQos(self: *Channel, prefetch_count: u16, global: bool) !void {
        try self.connection.sendMethod(self.id, .{ .basic_qos = .{
            .prefetch_count = prefetch_count,
            .global = global,
        } });
        const response = try self.awaitMethod();
        switch (response) {
            .basic_qos_ok => {
                // Record for recovery
                for (self.connection.topology.channels.items) |*rch| {
                    if (rch.id == self.id) {
                        rch.prefetch_count = prefetch_count;
                        rch.prefetch_global = global;
                        break;
                    }
                }
            },
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Start consuming from a queue. Returns the consumer tag.
    pub fn basicConsume(self: *Channel, queue: []const u8, consumer_tag: []const u8, ack_mode: AckMode) ![]const u8 {
        return self.basicConsumeWithArgs(queue, consumer_tag, ack_mode, false, FieldTable.empty);
    }

    pub fn basicConsumeWithArgs(self: *Channel, queue: []const u8, consumer_tag: []const u8, ack_mode: AckMode, exclusive: bool, arguments: FieldTable) ![]const u8 {
        try self.connection.sendMethod(self.id, .{ .basic_consume = .{
            .queue = queue,
            .consumer_tag = consumer_tag,
            .no_ack = ack_mode == .automatic,
            .exclusive = exclusive,
            .arguments = arguments,
        } });
        const response = try self.awaitMethod();
        return switch (response) {
            .basic_consume_ok => |ok| blk: {
                self.connection.topology.recordConsumer(self.allocator, .{
                    .queue = queue,
                    .consumer_tag = ok.consumer_tag,
                    .no_ack = ack_mode == .automatic,
                    .exclusive = exclusive,
                    .channel_id = self.id,
                }) catch {};
                break :blk ok.consumer_tag;
            },
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => error.ProtocolError,
        };
    }

    /// Start consuming with a callback handler. The callback is invoked for each delivery.
    pub fn basicConsumeWith(self: *Channel, queue: []const u8, consumer_tag: []const u8, ack_mode: AckMode, handler: *const fn (Delivery) void) ![]const u8 {
        const tag = try self.basicConsume(queue, consumer_tag, ack_mode);
        try self.consumer_callbacks.put(tag, handler);
        return tag;
    }

    /// Cancel a consumer.
    pub fn basicCancel(self: *Channel, consumer_tag: []const u8) !void {
        try self.connection.sendMethod(self.id, .{ .basic_cancel = .{ .consumer_tag = consumer_tag } });
        const response = try self.awaitMethod();
        switch (response) {
            .basic_cancel_ok => {
                _ = self.consumer_callbacks.remove(consumer_tag);
            },
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Receive the next delivery (blocking). Returns null if the channel is closed.
    pub fn recvDelivery(self: *Channel) !?Delivery {
        const io = getIo();
        self.delivery_mutex.lockUncancelable(io);
        defer self.delivery_mutex.unlock(io);

        while (self.delivery_head >= self.deliveries.items.len) {
            if (!self.is_open.load(.acquire)) return null;
            self.delivery_signal.waitUncancelable(io, &self.delivery_mutex);
        }

        const d = self.deliveries.items[self.delivery_head];
        self.delivery_head += 1;
        // Compact when fully drained
        if (self.delivery_head == self.deliveries.items.len) {
            self.deliveries.clearRetainingCapacity();
            self.delivery_head = 0;
        }
        return d;
    }

    /// Try to receive a delivery without blocking. Returns null if none available.
    pub fn tryRecvDelivery(self: *Channel) ?Delivery {
        const io = getIo();
        self.delivery_mutex.lockUncancelable(io);
        defer self.delivery_mutex.unlock(io);

        if (self.delivery_head >= self.deliveries.items.len) return null;
        const d = self.deliveries.items[self.delivery_head];
        self.delivery_head += 1;
        if (self.delivery_head == self.deliveries.items.len) {
            self.deliveries.clearRetainingCapacity();
            self.delivery_head = 0;
        }
        return d;
    }

    /// Synchronous fetch (basic.get). Returns null if the queue is empty.
    pub fn basicGet(self: *Channel, queue: []const u8, ack_mode: AckMode) !?GetResult {
        // Reset content state before sending to avoid races with the reader thread
        self.rpc_mutex.lockUncancelable(getIo());
        self.get_ready = false;
        self.rpc_mutex.unlock(getIo());

        try self.connection.sendMethod(self.id, .{ .basic_get = .{
            .queue = queue,
            .no_ack = ack_mode == .automatic,
        } });

        const response = try self.awaitMethod();
        switch (response) {
            .basic_get_ok => |ok| {
                // Wait for header + body
                const content = try self.awaitContent();
                return .{
                    .delivery_tag = ok.delivery_tag,
                    .redelivered = ok.redelivered,
                    .exchange = ok.exchange,
                    .routing_key = ok.routing_key,
                    .message_count = ok.message_count,
                    .properties = content.properties,
                    .body = content.body,
                };
            },
            .basic_get_empty => return null,
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    //
    // Acknowledgements
    //

    /// Acknowledge a delivery.
    pub fn basicAck(self: *Channel, delivery_tag: u64, multiple: bool) !void {
        try self.connection.sendMethod(self.id, .{ .basic_ack = .{
            .delivery_tag = delivery_tag,
            .multiple = multiple,
        } });
    }

    /// Acknowledge all deliveries up to and including this delivery tag.
    pub fn basicAckMultiple(self: *Channel, delivery_tag: u64) !void {
        return self.basicAck(delivery_tag, true);
    }

    /// Negatively acknowledge a delivery (RabbitMQ extension).
    pub fn basicNack(self: *Channel, delivery_tag: u64, multiple: bool, requeue: bool) !void {
        try self.connection.sendMethod(self.id, .{ .basic_nack = .{
            .delivery_tag = delivery_tag,
            .multiple = multiple,
            .requeue = requeue,
        } });
    }

    /// Reject a delivery.
    pub fn basicReject(self: *Channel, delivery_tag: u64, requeue: bool) !void {
        try self.connection.sendMethod(self.id, .{ .basic_reject = .{
            .delivery_tag = delivery_tag,
            .requeue = requeue,
        } });
    }

    //
    // Publisher confirms
    //

    pub const ConfirmSelectOptions = struct {
        /// When true, each publish call blocks until the broker confirms it
        tracking: bool = false,
        /// Maximum unconfirmed messages before publish blocks (0 = unlimited).
        /// Only meaningful when tracking is true.
        outstanding_limit: u32 = 0,
    };

    /// Enable publisher confirm mode on this channel.
    pub fn confirmSelect(self: *Channel) !void {
        return self.confirmSelectWithOptions(.{});
    }

    /// Enable publisher confirm mode with per-message tracking and optional backpressure.
    pub fn confirmSelectWithOptions(self: *Channel, opts: ConfirmSelectOptions) !void {
        try self.connection.sendMethod(self.id, .{ .confirm_select = .{} });
        const response = try self.awaitMethod();
        switch (response) {
            .confirm_select_ok => {
                self.confirm_mode = true;
                self.confirm_tracking = opts.tracking;
                self.outstanding_limit = opts.outstanding_limit;
                self.next_publish_seq_no = 0;
                self.outstanding_count = 0;
                self.last_confirmed_seq = 0;
                // Record for recovery
                for (self.connection.topology.channels.items) |*rch| {
                    if (rch.id == self.id) {
                        rch.confirm_mode = true;
                        break;
                    }
                }
            },
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Wait until all published messages have been confirmed.
    /// Only useful when tracking is false; with tracking, each publish already waits.
    pub fn waitForConfirms(self: *Channel) !bool {
        return self.waitForConfirmsTimeout(std.time.ns_per_s * 30);
    }

    /// Wait for confirms with a timeout (in nanoseconds).
    pub fn waitForConfirmsTimeout(self: *Channel, timeout_ns: u64) !bool {
        const io = getIo();
        const start = Io.Timestamp.now(io, .boot);
        const limit: i96 = @intCast(timeout_ns);

        self.confirm_mutex.lockUncancelable(io);
        defer self.confirm_mutex.unlock(io);

        const target = self.next_publish_seq_no;
        while (self.last_confirmed_seq < target) {
            if (!self.is_open.load(.acquire)) return error.ChannelClosed;
            const elapsed = Io.Timestamp.now(io, .boot).nanoseconds - start.nanoseconds;
            if (elapsed >= limit) return error.Timeout;
            const remaining: Io.Duration = .{ .nanoseconds = limit - elapsed };
            const timeout: Io.Timeout = .{ .duration = .{ .raw = remaining, .clock = .awake } };
            const epoch = self.confirm_signal.epoch.load(.acquire);
            self.confirm_mutex.unlock(io);
            io.futexWaitTimeout(u32, &self.confirm_signal.epoch.raw, epoch, timeout) catch {};
            self.confirm_mutex.lockUncancelable(io);
        }

        return true;
    }

    //
    // Transactions
    //

    /// Enable transaction mode.
    pub fn txSelect(self: *Channel) !void {
        try self.connection.sendMethod(self.id, .{ .tx_select = {} });
        const response = try self.awaitMethod();
        switch (response) {
            .tx_select_ok => {},
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Commit the current transaction.
    pub fn txCommit(self: *Channel) !void {
        try self.connection.sendMethod(self.id, .{ .tx_commit = {} });
        const response = try self.awaitMethod();
        switch (response) {
            .tx_commit_ok => {},
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    /// Rollback the current transaction.
    pub fn txRollback(self: *Channel) !void {
        try self.connection.sendMethod(self.id, .{ .tx_rollback = {} });
        const response = try self.awaitMethod();
        switch (response) {
            .tx_rollback_ok => {},
            .channel_close => |cc| {
                try self.handleChannelClose(cc);
                return error.ChannelClosed;
            },
            else => return error.ProtocolError,
        }
    }

    //
    // Channel lifecycle
    //

    /// Close this channel gracefully.
    pub fn closeChannel(self: *Channel) !void {
        if (!self.is_open.load(.acquire)) return;

        self.connection.sendMethod(self.id, .{ .channel_close = .{
            .reply_code = 200,
            .reply_text = "Normal close",
            .class_id = 0,
            .method_id = 0,
        } }) catch {
            self.is_open.store(false, .release);
            self.connection.removeChannel(self.id);
            return;
        };

        // Wait for close-ok with a short timeout
        _ = self.awaitMethodTimeout(5 * std.time.ns_per_s) catch null;
        self.is_open.store(false, .release);
        self.connection.removeChannel(self.id);
    }

    /// Whether this channel is open.
    pub fn isOpen(self: *const Channel) bool {
        return self.is_open.load(.acquire);
    }

    /// Set a consumer work pool for dispatching callback deliveries off the reader thread.
    pub fn setWorkPool(self: *Channel, pool: *ConsumerWorkPool) void {
        self.work_pool = pool;
    }

    /// Register a listener for channel events.
    pub fn addEventListener(self: *Channel, cb: *const fn (ChannelEvent) void) !void {
        try self.event_listeners.add(self.allocator, cb);
    }

    /// Internal close (called during connection shutdown, does not send to server).
    pub fn closeInternal(self: *Channel) void {
        self.is_open.store(false, .release);
        const io = getIo();

        self.delivery_mutex.lockUncancelable(io);
        self.delivery_signal.broadcast(io);
        self.delivery_mutex.unlock(io);

        self.rpc_mutex.lockUncancelable(io);
        self.rpc_signal.broadcast(io);
        self.rpc_mutex.unlock(io);

        // Resolve all outstanding confirm promises so blocked publishers wake up
        self.confirm_mutex.lockUncancelable(io);
        var it = self.confirm_promises.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.result = .nacked;
            entry.value_ptr.*.event.set(io);
        }
        self.confirm_signal.broadcast(io);
        self.confirm_mutex.unlock(io);
    }

    /// Process a publisher confirm from the server.
    fn handleConfirm(self: *Channel, delivery_tag: u64, multiple: bool, ack: bool) void {
        const io = getIo();
        self.confirm_mutex.lockUncancelable(io);

        const prev_confirmed = self.last_confirmed_seq;
        if (delivery_tag > self.last_confirmed_seq) {
            self.last_confirmed_seq = delivery_tag;
        }

        // Resolve individual promises
        const start = if (multiple) prev_confirmed + 1 else delivery_tag;
        var seq = start;
        var resolved_count: u32 = 0;
        while (seq <= delivery_tag) : (seq += 1) {
            if (self.confirm_promises.fetchRemove(seq)) |kv| {
                kv.value.result = if (ack) .acked else .nacked;
                kv.value.event.set(io);
                resolved_count += 1;
            }
        }

        if (self.confirm_tracking and self.outstanding_limit > 0) {
            self.outstanding_count -|= resolved_count;
        }

        // Wake backpressure waiters and batch waitForConfirms waiters
        self.confirm_signal.broadcast(io);
        self.confirm_mutex.unlock(io);
    }

    //
    // Frame handling (called by reader thread)
    //

    pub fn handleFrame(self: *Channel, frame: frame_mod.Frame) void {
        switch (frame) {
            .method => |mf| self.handleMethod(mf.method),
            .header => |hf| {
                self.pending_header = hf;
                self.pending_body.clearRetainingCapacity();
                // Pre-allocate for the full body to avoid per-frame reallocation
                if (hf.body_size > 0) {
                    self.pending_body.ensureTotalCapacity(self.allocator, @intCast(hf.body_size)) catch {};
                }
                if (hf.body_size == 0) {
                    self.assembleContent();
                }
            },
            .body => |bf| {
                self.pending_body.appendSlice(self.allocator, bf.payload) catch return;
                if (self.pending_header) |hdr| {
                    if (self.pending_body.items.len >= hdr.body_size) {
                        self.assembleContent();
                    }
                }
            },
            .heartbeat => {},
        }
    }

    fn handleMethod(self: *Channel, m: Method) void {
        switch (m) {
            .basic_deliver => {
                // Store the deliver method, wait for header + body
                self.pending_method = m;
            },
            .basic_return => {
                self.pending_method = m;
            },
            .basic_ack => |ack| {
                self.handleConfirm(ack.delivery_tag, ack.multiple, true);
            },
            .basic_nack => |nack| {
                self.handleConfirm(nack.delivery_tag, nack.multiple, false);
            },
            .basic_cancel => |cancel| {
                log.info("consumer cancelled by server on channel {d}", .{self.id});
                _ = self.consumer_callbacks.remove(cancel.consumer_tag);
                if (self.on_cancel) |cb| cb(cancel.consumer_tag);
                self.event_listeners.emit(.{ .consumer_cancelled = cancel.consumer_tag });
            },
            .channel_close => |cc| {
                // Send close-ok immediately from the reader thread
                self.connection.sendMethod(self.id, .{ .channel_close_ok = {} }) catch {};
                self.connection.removeChannel(self.id);
                log.err("channel {d} closed by server: [{d}] {s}", .{ self.id, cc.reply_code, cc.reply_text });
                // Post as RPC response so any waiting caller sees it
                self.rpc_mutex.lockUncancelable(getIo());
                self.rpc_response = m;
                self.rpc_signal.signal(getIo());
                self.rpc_mutex.unlock(getIo());
                self.event_listeners.emit(.{ .closed = .{
                    .code = cc.reply_code,
                    .text = cc.reply_text,
                    .initiated_by_server = true,
                } });
                self.closeInternal();
            },
            .channel_flow => |flow| {
                self.connection.sendMethod(self.id, .{ .channel_flow_ok = .{ .active = flow.active } }) catch {};
                self.event_listeners.emit(.{ .flow = flow.active });
            },
            else => {
                // RPC response
                self.rpc_mutex.lockUncancelable(getIo());
                self.rpc_response = m;
                self.rpc_signal.signal(getIo());
                self.rpc_mutex.unlock(getIo());
            },
        }
    }

    fn assembleContent(self: *Channel) void {
        const header = self.pending_header orelse return;
        const body = self.pending_body.items;

        if (self.pending_method) |m| {
            switch (m) {
                .basic_deliver => |deliver| {
                    // Transfer ownership of pending_body to the delivery (zero copy)
                    const owned_body = self.pending_body.toOwnedSlice(self.allocator) catch
                        self.allocator.dupe(u8, body) catch return;
                    const delivery = Delivery{
                        .consumer_tag = deliver.consumer_tag,
                        .delivery_tag = deliver.delivery_tag,
                        .redelivered = deliver.redelivered,
                        .exchange = deliver.exchange,
                        .routing_key = deliver.routing_key,
                        .properties = header.properties,
                        .body = owned_body,
                    };

                    if (self.consumer_callbacks.get(deliver.consumer_tag)) |cb| {
                        if (self.work_pool) |pool| {
                            pool.submit(cb, delivery);
                        } else {
                            cb(delivery);
                        }
                    } else {
                        self.delivery_mutex.lockUncancelable(getIo());
                        self.deliveries.append(self.allocator, delivery) catch {
                            log.err("failed to enqueue delivery: out of memory", .{});
                            return;
                        };
                        self.delivery_signal.signal(getIo());
                        self.delivery_mutex.unlock(getIo());
                    }
                },
                .basic_return => |ret| {
                    const returned = ReturnedMessage{
                        .reply_code = ret.reply_code,
                        .reply_text = ret.reply_text,
                        .exchange = ret.exchange,
                        .routing_key = ret.routing_key,
                        .properties = header.properties,
                        .body = self.allocator.dupe(u8, body) catch return,
                    };
                    self.returns.append(self.allocator, returned) catch {
                        log.err("failed to enqueue returned message: out of memory", .{});
                    };
                    if (self.on_return) |cb| cb(returned);
                },
                else => {},
            }
            self.pending_method = null;
        } else {
            // Content for basic.get-ok — store and signal
            self.get_properties = header.properties;
            self.get_body.clearRetainingCapacity();
            self.get_body.appendSlice(self.allocator, body) catch {};
            self.get_ready = true;

            self.rpc_mutex.lockUncancelable(getIo());
            self.rpc_signal.signal(getIo());
            self.rpc_mutex.unlock(getIo());
        }

        self.pending_header = null;
        self.pending_body.clearRetainingCapacity();
    }

    /// Wait for the next RPC response (used by synchronous operations).
    pub fn awaitMethod(self: *Channel) !Method {
        return self.awaitMethodTimeout(15 * std.time.ns_per_s);
    }

    fn awaitMethodTimeout(self: *Channel, timeout_ns: u64) !Method {
        const io = getIo();
        const start = Io.Timestamp.now(io, .boot);
        const limit: i96 = @intCast(timeout_ns);

        self.rpc_mutex.lockUncancelable(io);
        defer self.rpc_mutex.unlock(io);

        while (self.rpc_response == null) {
            if (!self.is_open.load(.acquire)) return error.ChannelClosed;
            const elapsed = Io.Timestamp.now(io, .boot).nanoseconds - start.nanoseconds;
            if (elapsed >= limit) return error.Timeout;
            const remaining: Io.Duration = .{ .nanoseconds = limit - elapsed };
            const timeout: Io.Timeout = .{ .duration = .{ .raw = remaining, .clock = .awake } };
            // Snapshot the epoch, release the mutex, and do a timed futex
            // wait. This replicates Condition.wait with a timeout bound.
            const epoch = self.rpc_signal.epoch.load(.acquire);
            self.rpc_mutex.unlock(io);
            io.futexWaitTimeout(u32, &self.rpc_signal.epoch.raw, epoch, timeout) catch {};
            self.rpc_mutex.lockUncancelable(io);
        }
        const response = self.rpc_response.?;
        self.rpc_response = null;
        return response;
    }

    const ContentResult = struct {
        properties: BasicProperties,
        body: []const u8,
    };

    /// Wait for content frames (header + body) to be assembled for basic.get-ok.
    fn awaitContent(self: *Channel) !ContentResult {
        const io = getIo();
        const timeout_ns: u64 = 30 * std.time.ns_per_s;
        const start = Io.Timestamp.now(io, .boot);
        const limit: i96 = @intCast(timeout_ns);

        self.rpc_mutex.lockUncancelable(io);
        defer self.rpc_mutex.unlock(io);

        while (!self.get_ready) {
            if (!self.is_open.load(.acquire)) return error.ChannelClosed;
            const elapsed = Io.Timestamp.now(io, .boot).nanoseconds - start.nanoseconds;
            if (elapsed >= limit) return error.Timeout;
            const remaining: Io.Duration = .{ .nanoseconds = limit - elapsed };
            const timeout: Io.Timeout = .{ .duration = .{ .raw = remaining, .clock = .awake } };
            const epoch = self.rpc_signal.epoch.load(.acquire);
            self.rpc_mutex.unlock(io);
            io.futexWaitTimeout(u32, &self.rpc_signal.epoch.raw, epoch, timeout) catch {};
            self.rpc_mutex.lockUncancelable(io);
        }

        return .{
            .properties = self.get_properties,
            .body = self.get_body.items,
        };
    }

    /// Called by RPC waiters that received channel_close as their response.
    /// The reader thread (handleMethod) already sent close-ok, emitted
    /// the event, and called closeInternal, so this is a no-op.
    fn handleChannelClose(_: *Channel, _: method_mod.ChannelClose) !void {}
};
