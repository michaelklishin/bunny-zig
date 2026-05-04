/// AMQP 0-9-1 channel: the primary surface for queue, exchange, publish, and consume operations.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Thread = std.Thread;
const Io = std.Io;
const Mutex = Io.Mutex;
const Condition = Io.Condition;
const Notify = @import("notify.zig").Notify;

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
const connection_mod = @import("connection.zig");
const Connection = connection_mod.Connection;
const ChannelError = connection_mod.ChannelError;
const Queue = @import("queue.zig").Queue;
const Exchange = @import("exchange.zig").Exchange;
const recovery_mod = @import("recovery.zig");
const events = @import("events.zig");
const ChannelEvent = events.ChannelEvent;
const ConsumerWorkPool = @import("consumer_work_pool.zig").ConsumerWorkPool;

const log = std.log.scoped(.bunny_channel);

/// A delivered message. Owns its storage, call `deinit` when done.
/// A message pushed to a consumer.
///
/// Borrows `*Channel` so `ack`, `nack`, `respond`, etc. route on the originating
/// channel. The borrow is valid for the connection's lifetime; closing the
/// channel alone makes per-message helpers return `error.ChannelClosed`,
/// `Connection.deinit` invalidates outstanding `Delivery` values.
///
/// Owns `consumer_tag`, `exchange`, `routing_key`, `properties`, and `body`;
/// caller must `deinit(allocator)`.
pub const Delivery = struct {
    /// Borrowed for the connection's lifetime. See struct-level note.
    channel: *Channel,
    consumer_tag: []const u8,
    delivery_tag: u64,
    redelivered: bool,
    exchange: []const u8,
    routing_key: []const u8,
    properties: BasicProperties,
    body: []const u8,

    /// Free the storage owned by this delivery. By value so `defer raw.deinit(...)`
    /// works directly on a while-let capture without an intermediate `var`.
    pub fn deinit(self: Delivery, allocator: std.mem.Allocator) void {
        allocator.free(self.consumer_tag);
        allocator.free(self.exchange);
        allocator.free(self.routing_key);
        var props = self.properties;
        props.deinitOwned(allocator);
        if (self.body.len > 0) allocator.free(self.body);
    }

    /// Reply to this request-reply request. Publishes `body` to the default
    /// exchange with `delivery.properties.reply_to` as the routing key and
    /// propagates `delivery.properties.correlation_id`. See `Channel.respondTo`.
    pub fn respond(self: Delivery, body: []const u8) !void {
        return self.channel.respondTo(self, body);
    }

    /// Acknowledge this delivery on its originating channel.
    pub fn ack(self: Delivery) !void {
        return self.channel.ack(self.delivery_tag);
    }

    /// Negatively acknowledge this delivery without requeueing (drop or dead-letter).
    pub fn nack(self: Delivery) !void {
        return self.channel.nack(self.delivery_tag);
    }

    /// Negatively acknowledge this delivery and ask the broker to requeue it.
    pub fn nackRequeue(self: Delivery) !void {
        return self.channel.nackRequeue(self.delivery_tag);
    }

    /// Reject this delivery without requeueing.
    pub fn reject(self: Delivery) !void {
        return self.channel.reject(self.delivery_tag);
    }

    /// Reject this delivery and ask the broker to requeue it.
    pub fn rejectRequeue(self: Delivery) !void {
        return self.channel.rejectRequeue(self.delivery_tag);
    }
};

/// Result of `basic.get` (polling alternative to `basic.consume`).
///
/// Same `*Channel` borrow contract as `Delivery`. Owns `exchange`,
/// `routing_key`, `properties`, and `body`; caller must `deinit(allocator)`.
pub const BasicGetResult = struct {
    /// Borrowed for the connection's lifetime. See `Delivery`.
    channel: *Channel,
    delivery_tag: u64,
    redelivered: bool,
    exchange: []const u8,
    routing_key: []const u8,
    message_count: u32,
    properties: BasicProperties,
    body: []const u8,

    pub fn deinit(self: BasicGetResult, allocator: std.mem.Allocator) void {
        allocator.free(self.exchange);
        allocator.free(self.routing_key);
        var props = self.properties;
        props.deinitOwned(allocator);
        if (self.body.len > 0) allocator.free(self.body);
    }

    /// Acknowledge this delivery on its originating channel.
    pub fn ack(self: BasicGetResult) !void {
        return self.channel.ack(self.delivery_tag);
    }

    /// Negatively acknowledge this delivery without requeueing (drop or dead-letter).
    pub fn nack(self: BasicGetResult) !void {
        return self.channel.nack(self.delivery_tag);
    }

    /// Negatively acknowledge this delivery and ask the broker to requeue it.
    pub fn nackRequeue(self: BasicGetResult) !void {
        return self.channel.nackRequeue(self.delivery_tag);
    }

    /// Reject this delivery without requeueing.
    pub fn reject(self: BasicGetResult) !void {
        return self.channel.reject(self.delivery_tag);
    }

    /// Reject this delivery and ask the broker to requeue it.
    pub fn rejectRequeue(self: BasicGetResult) !void {
        return self.channel.rejectRequeue(self.delivery_tag);
    }
};

/// Server-initiated channel.close details, surfaced after a typed error
/// returns from an API call. Owns `reply_text`.
pub const ChannelCloseInfo = struct {
    reply_code: u16,
    reply_text: []const u8,
    class_id: u16,
    method_id: u16,
    initiated_by_server: bool,

    pub fn deinit(self: ChannelCloseInfo, allocator: std.mem.Allocator) void {
        // reply_text is a sentinel empty slice when the dupe at record time failed,
        // and that slice does not belong to `allocator`. Skip the free in that case.
        if (self.reply_text.len > 0) allocator.free(self.reply_text);
    }
};

/// Map an AMQP 0-9-1 reply code to a typed error. Codes outside the spec set,
/// and the success code 200, fall back to `error.ChannelClosed`.
pub fn replyCodeToError(reply_code: u16) ChannelError {
    return switch (reply_code) {
        311 => error.ContentTooLarge,
        313 => error.NoConsumers,
        320 => error.ConnectionForced,
        402 => error.InvalidPath,
        403 => error.AccessRefused,
        404 => error.NotFound,
        405 => error.ResourceLocked,
        406 => error.PreconditionFailed,
        501 => error.FrameError,
        502 => error.SyntaxError,
        503 => error.CommandInvalid,
        505 => error.UnexpectedFrame,
        506 => error.ResourceError,
        530 => error.NotAllowed,
        540 => error.NotImplemented,
        541 => error.InternalError,
        else => error.ChannelClosed,
    };
}

/// A returned message (mandatory/immediate failure). Owns its storage, call `deinit` when done.
pub const ReturnedMessage = struct {
    reply_code: u16,
    reply_text: []const u8,
    exchange: []const u8,
    routing_key: []const u8,
    properties: BasicProperties,
    body: []const u8,

    pub fn deinit(self: ReturnedMessage, allocator: std.mem.Allocator) void {
        allocator.free(self.reply_text);
        allocator.free(self.exchange);
        allocator.free(self.routing_key);
        var props = self.properties;
        props.deinitOwned(allocator);
        if (self.body.len > 0) allocator.free(self.body);
    }
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

/// An AMQP 0-9-1 channel. All queue, exchange, publish, and consume operations happen on a channel.
pub const Channel = struct {
    allocator: Allocator,
    connection: *Connection,
    id: u16,
    is_open: std.atomic.Value(bool) = std.atomic.Value(bool).init(true),

    // RPC synchronization: the reader thread posts responses here
    rpc_mutex: Mutex = .init,
    rpc_signal: Notify = .init,
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

    // Returned messages: dispatched to on_return if set, dropped otherwise.
    // The framework deinits the message after the handler returns.
    on_return: ?*const fn (ReturnedMessage) void = null,
    on_cancel: ?*const fn ([]const u8) void = null,

    // All consumers registered on this channel. Keys are owned, duped from the
    // broker's `basic.consume-ok` reply. Value is the optional callback for
    // push-style consumers; null means deliveries go to the recvDelivery queue.
    consumers: std.StringHashMap(?*const fn (Delivery) void) = undefined,

    // Channel event listeners
    event_listeners: events.EventListeners(ChannelEvent) = .{},

    // Server-initiated close, set by the reader thread, surfaced via lastClose.
    close_info_mutex: Mutex = .init,
    last_close: ?ChannelCloseInfo = null,

    // Consumer work pool (optional, for dispatching callback consumers off the reader thread)
    work_pool: ?*ConsumerWorkPool = null,

    // Publisher confirms
    confirm_mode: bool = false,
    confirm_tracking: bool = false,
    outstanding_limit: u32 = 0,
    outstanding_count: u32 = 0,
    next_publish_seq_no: u64 = 0,
    confirm_mutex: Mutex = .init,
    confirm_signal: Notify = .init,
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
            .consumers = std.StringHashMap(?*const fn (Delivery) void).init(allocator),
        };
        return ch;
    }

    pub fn deinit(self: *Channel) void {
        self.pending_body.deinit(self.allocator);
        self.get_body.deinit(self.allocator);
        self.get_properties.deinitOwned(self.allocator);
        // Drop any deliveries the application never received.
        for (self.deliveries.items[self.delivery_head..]) |d| d.deinit(self.allocator);
        self.deliveries.deinit(self.allocator);
        self.confirm_promises.deinit();
        for (self.promise_pool.items) |p| self.allocator.destroy(p);
        self.promise_pool.deinit(self.allocator);
        var cb_it = self.consumers.keyIterator();
        while (cb_it.next()) |k| self.allocator.free(k.*);
        self.consumers.deinit();
        self.event_listeners.deinit(self.allocator);
        if (self.last_close) |info| info.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    //
    // Queue operations
    //

    /// Declare a queue and return a handle for subsequent operations.
    ///
    /// The returned `Queue` carries `name`, `message_count`, and
    /// `consumer_count`. For caller-named queues the `name` aliases the
    /// caller's slice and `deinit` is a no-op. For server-named queues
    /// (empty `name`) the broker-assigned name is duped, and the caller
    /// must call `Queue.deinit(allocator)` to release it.
    pub fn queueDeclare(self: *Channel, name: []const u8, opts: QueueDeclareOptions) !Queue {
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
                // For server-named queues, ok.queue aliases the response frame
                // buffer and is invalidated by the next RPC; dupe so the handle's
                // name outlives subsequent calls.
                const stable_name = if (name.len > 0) name else try self.allocator.dupe(u8, ok.queue);
                break :blk .{
                    .channel = self,
                    .name = stable_name,
                    .owns_name = name.len == 0,
                    .message_count = ok.message_count,
                    .consumer_count = ok.consumer_count,
                };
            },
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => error.ProtocolError,
        };
    }

    /// Declare a durable queue.
    pub fn durableQueue(self: *Channel, name: []const u8) !Queue {
        return self.queueDeclare(name, QueueDeclareOptions.durableQueue());
    }

    /// Assert a queue exists without modifying it. Closes the channel with
    /// NOT_FOUND (404) if the queue is missing.
    pub fn queueDeclarePassive(self: *Channel, name: []const u8) !Queue {
        return self.queueDeclare(name, .{ .passive = true });
    }

    /// Declare a quorum queue.
    pub fn quorumQueue(self: *Channel, name: []const u8) !Queue {
        const entries = [_]FieldTable.Entry{
            .{ .key = "x-queue-type", .value = .{ .long_string = QueueType.quorum } },
        };
        var args = try FieldTable.fromEntries(self.allocator, &entries);
        defer args.deinit();
        return self.queueDeclare(name, .{ .durable = true, .arguments = args });
    }

    /// Declare a stream queue.
    pub fn streamQueue(self: *Channel, name: []const u8) !Queue {
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
    pub fn delayedQueue(self: *Channel, name: []const u8, opts: QueueDeclareOptions) !Queue {
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
    pub fn jmsQueue(self: *Channel, name: []const u8, opts: QueueDeclareOptions) !Queue {
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
        return self.basicConsumeWithTagAndArgs(queue_name, consumer_tag, ack_mode, false, args);
    }

    /// Declare a temporary (exclusive, auto-delete) queue with a server-generated name.
    /// The returned `Queue` owns its name; call `Queue.deinit(allocator)` when done.
    pub fn temporaryQueue(self: *Channel) !Queue {
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
            .channel_close => |cc| return self.handleChannelClose(cc),
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
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => return error.ProtocolError,
        }
    }

    /// Purge a queue. Returns the number of messages purged.
    pub fn queuePurge(self: *Channel, queue: []const u8) !u32 {
        try self.connection.sendMethod(self.id, .{ .queue_purge = .{ .queue = queue } });
        const response = try self.awaitMethod();
        return switch (response) {
            .queue_purge_ok => |ok| ok.message_count,
            .channel_close => |cc| return self.handleChannelClose(cc),
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
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => error.ProtocolError,
        };
    }

    //
    // Exchange operations
    //

    /// Declare an exchange and return a handle for subsequent operations.
    /// The handle's `name` aliases the caller's slice; no allocation is owned.
    pub fn exchangeDeclare(self: *Channel, name: []const u8, exchange_type: []const u8, opts: ExchangeDeclareOptions) !Exchange {
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
                return .{ .channel = self, .name = name };
            },
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => return error.ProtocolError,
        }
    }

    /// Declare a durable direct exchange.
    pub fn declareDirectExchange(self: *Channel, name: []const u8) !Exchange {
        return self.exchangeDeclare(name, ExchangeType.direct, ExchangeDeclareOptions.durableExchange());
    }

    /// Declare a durable fanout exchange.
    pub fn declareFanoutExchange(self: *Channel, name: []const u8) !Exchange {
        return self.exchangeDeclare(name, ExchangeType.fanout, ExchangeDeclareOptions.durableExchange());
    }

    /// Declare a durable topic exchange.
    pub fn declareTopicExchange(self: *Channel, name: []const u8) !Exchange {
        return self.exchangeDeclare(name, ExchangeType.topic, ExchangeDeclareOptions.durableExchange());
    }

    /// Declare a durable headers exchange.
    pub fn declareHeadersExchange(self: *Channel, name: []const u8) !Exchange {
        return self.exchangeDeclare(name, ExchangeType.headers, ExchangeDeclareOptions.durableExchange());
    }

    /// Assert an exchange exists without modifying it. Closes the channel with
    /// NOT_FOUND (404) if the exchange is missing. The exchange_type argument is
    /// sent for protocol completeness but is ignored by the broker for passive declares.
    pub fn exchangeDeclarePassive(self: *Channel, name: []const u8) !Exchange {
        return self.exchangeDeclare(name, ExchangeType.direct, .{ .passive = true });
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
            .channel_close => |cc| return self.handleChannelClose(cc),
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
            .channel_close => |cc| return self.handleChannelClose(cc),
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
            .channel_close => |cc| return self.handleChannelClose(cc),
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
                        return self.typedClosedError();
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

    /// Reply to a request-reply request. Publishes `body` to the default
    /// exchange using `delivery.properties.reply_to` as the routing key, and
    /// propagates `delivery.properties.correlation_id` so the client can match
    /// the response. Returns `error.NoReplyTo` if the delivery has no
    /// `reply_to` set.
    pub fn respondTo(self: *Channel, delivery: Delivery, body: []const u8) !void {
        const reply_to = delivery.properties.reply_to orelse return error.NoReplyTo;
        var props = BasicProperties.default;
        if (delivery.properties.correlation_id) |cid| props = props.withCorrelationId(cid);
        return self.publish(body, .{
            .exchange = "",
            .routing_key = reply_to,
            .properties = props,
        });
    }

    /// Publish a batch of messages to the same exchange and routing key.
    /// Use opts.flush to control whether to flush after the batch.
    pub fn publishBatch(
        self: *Channel,
        bodies: []const []const u8,
        opts: PublishOptions,
    ) !void {
        if (bodies.len == 0) return;

        if (self.confirm_mode) {
            const io = getIo();
            self.confirm_mutex.lockUncancelable(io);

            if (self.confirm_tracking and self.outstanding_limit > 0) {
                while (self.outstanding_count >= self.outstanding_limit) {
                    if (!self.is_open.load(.acquire)) {
                        self.confirm_mutex.unlock(io);
                        return self.typedClosedError();
                    }
                    self.confirm_signal.waitUncancelable(io, &self.confirm_mutex);
                }
            }

            self.next_publish_seq_no += @intCast(bodies.len);
            if (self.confirm_tracking) {
                self.outstanding_count += @intCast(bodies.len);
            }
            self.confirm_mutex.unlock(io);
        }

        const method = protocol.method.Method{ .basic_publish = .{
            .exchange = opts.exchange,
            .routing_key = opts.routing_key,
            .mandatory = opts.mandatory,
        } };

        try self.connection.sendPublishBatch(self.id, method, opts.properties, bodies, opts.flush == .flush_immediately);
    }

    /// Flush the connection's write buffer to the socket.
    /// Only needed when using FlushStrategy.buffered.
    pub fn flushTransport(self: *Channel) !void {
        try self.connection.flushWrite();
    }

    //
    // Consuming
    //

    /// Set per-consumer prefetch count, the common case for QoS.
    /// Equivalent to `basicQos(n, false)`.
    pub fn prefetch(self: *Channel, count: u16) !void {
        return self.basicQos(count, false);
    }

    /// Set QoS prefetch count. Prefer `prefetch(n)` for the per-consumer case.
    /// Note: `global = true` is denied by default starting with RabbitMQ 4.3.0
    /// (the `global_qos` deprecated feature is now denied by default).
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
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => return error.ProtocolError,
        }
    }

    /// Start consuming with a server-generated consumer tag.
    pub fn basicConsume(self: *Channel, queue: []const u8, ack_mode: AckMode) ![]const u8 {
        return self.basicConsumeWithTagAndArgs(queue, "", ack_mode, false, FieldTable.empty);
    }

    /// Start consuming with an explicit consumer tag.
    pub fn basicConsumeWithTag(self: *Channel, queue: []const u8, consumer_tag: []const u8, ack_mode: AckMode) ![]const u8 {
        return self.basicConsumeWithTagAndArgs(queue, consumer_tag, ack_mode, false, FieldTable.empty);
    }

    /// Start consuming with a server-generated consumer tag, exclusive flag, and arguments.
    pub fn basicConsumeWithArgs(self: *Channel, queue: []const u8, ack_mode: AckMode, exclusive: bool, arguments: FieldTable) ![]const u8 {
        return self.basicConsumeWithTagAndArgs(queue, "", ack_mode, exclusive, arguments);
    }

    /// Start consuming with an explicit consumer tag, exclusive flag, and arguments.
    /// Pass an empty `consumer_tag` to let the broker generate one.
    /// The returned tag is owned by the channel and stays valid until
    /// `basicCancel(tag)` or channel close.
    pub fn basicConsumeWithTagAndArgs(self: *Channel, queue: []const u8, consumer_tag: []const u8, ack_mode: AckMode, exclusive: bool, arguments: FieldTable) ![]const u8 {
        return self.registerConsumer(queue, consumer_tag, ack_mode, exclusive, arguments, null);
    }

    /// Common path for all `basicConsume*` variants: send `basic.consume`,
    /// dupe the broker-returned tag for stable lifetime, and register the
    /// consumer (with optional callback) in the channel's consumers map.
    fn registerConsumer(
        self: *Channel,
        queue: []const u8,
        consumer_tag: []const u8,
        ack_mode: AckMode,
        exclusive: bool,
        arguments: FieldTable,
        handler: ?*const fn (Delivery) void,
    ) ![]const u8 {
        try self.connection.sendMethod(self.id, .{ .basic_consume = .{
            .queue = queue,
            .consumer_tag = consumer_tag,
            .no_ack = ack_mode == .automatic,
            .exclusive = exclusive,
            .arguments = arguments,
        } });
        const response = try self.awaitMethod();
        switch (response) {
            .basic_consume_ok => |ok| {
                // ok.consumer_tag aliases the response read buffer. Dupe so the
                // returned slice (and the consumers-map key) outlive the next RPC.
                const owned_tag = try self.allocator.dupe(u8, ok.consumer_tag);
                errdefer self.allocator.free(owned_tag);
                try self.consumers.put(owned_tag, handler);
                const qos = self.recordedPrefetch();
                self.connection.topology.recordConsumer(self.allocator, .{
                    .queue = queue,
                    .consumer_tag = owned_tag,
                    .no_ack = ack_mode == .automatic,
                    .exclusive = exclusive,
                    .channel_id = self.id,
                    .prefetch_count = qos.count,
                    .prefetch_global = qos.global,
                }) catch {};
                return owned_tag;
            },
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => return error.ProtocolError,
        }
    }

    fn recordedPrefetch(self: *Channel) struct { count: u16, global: bool } {
        for (self.connection.topology.channels.items) |rch| {
            if (rch.id == self.id) return .{ .count = rch.prefetch_count, .global = rch.prefetch_global };
        }
        return .{ .count = 0, .global = false };
    }

    /// Start consuming with a callback handler and a server-generated consumer tag.
    pub fn basicConsumeWith(self: *Channel, queue: []const u8, ack_mode: AckMode, handler: *const fn (Delivery) void) ![]const u8 {
        return self.basicConsumeWithTagAndHandler(queue, "", ack_mode, handler);
    }

    /// Start consuming with a callback handler and an explicit consumer tag.
    /// The returned tag is owned by the channel and stays valid until cancellation.
    pub fn basicConsumeWithTagAndHandler(self: *Channel, queue: []const u8, consumer_tag: []const u8, ack_mode: AckMode, handler: *const fn (Delivery) void) ![]const u8 {
        return self.registerConsumer(queue, consumer_tag, ack_mode, false, FieldTable.empty, handler);
    }

    /// Cancel a consumer.
    pub fn basicCancel(self: *Channel, consumer_tag: []const u8) !void {
        try self.connection.sendMethod(self.id, .{ .basic_cancel = .{ .consumer_tag = consumer_tag } });
        const response = try self.awaitMethod();
        switch (response) {
            .basic_cancel_ok => {
                if (self.consumers.fetchRemove(consumer_tag)) |kv| self.allocator.free(kv.key);
            },
            .channel_close => |cc| return self.handleChannelClose(cc),
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
    /// The caller owns the result and must call `deinit`.
    pub fn basicGet(self: *Channel, queue: []const u8, ack_mode: AckMode) !?BasicGetResult {
        // Reset content state before sending to avoid races with the reader thread.
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
                const a = self.allocator;
                // ok.exchange/routing_key alias the read buffer; dupe before it is reused.
                const exchange = try a.dupe(u8, ok.exchange);
                errdefer a.free(exchange);
                const routing_key = try a.dupe(u8, ok.routing_key);
                errdefer a.free(routing_key);
                const content = try self.takeOwnedGetContent();
                return .{
                    .channel = self,
                    .delivery_tag = ok.delivery_tag,
                    .redelivered = ok.redelivered,
                    .exchange = exchange,
                    .routing_key = routing_key,
                    .message_count = ok.message_count,
                    .properties = content.properties,
                    .body = content.body,
                };
            },
            .basic_get_empty => return null,
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => return error.ProtocolError,
        }
    }

    //
    // Acknowledgements
    //

    /// Acknowledge a single delivery. Convenience wrapper over `basicAck`.
    pub fn ack(self: *Channel, delivery_tag: u64) !void {
        return self.basicAck(delivery_tag, false);
    }

    /// Acknowledge all deliveries up to and including this delivery tag.
    pub fn ackUpTo(self: *Channel, delivery_tag: u64) !void {
        return self.basicAck(delivery_tag, true);
    }

    /// Negatively acknowledge a single delivery without requeueing
    /// (drop or dead-letter). Convenience wrapper over `basicNack`.
    pub fn nack(self: *Channel, delivery_tag: u64) !void {
        return self.basicNack(delivery_tag, false, false);
    }

    /// Negatively acknowledge a single delivery and ask the broker to requeue it.
    pub fn nackRequeue(self: *Channel, delivery_tag: u64) !void {
        return self.basicNack(delivery_tag, false, true);
    }

    /// Reject a single delivery without requeueing (drop or dead-letter).
    /// Convenience wrapper over `basicReject`.
    pub fn reject(self: *Channel, delivery_tag: u64) !void {
        return self.basicReject(delivery_tag, false);
    }

    /// Reject a single delivery and ask the broker to requeue it.
    pub fn rejectRequeue(self: *Channel, delivery_tag: u64) !void {
        return self.basicReject(delivery_tag, true);
    }

    /// Acknowledge a delivery.
    pub fn basicAck(self: *Channel, delivery_tag: u64, multiple: bool) !void {
        try self.connection.sendMethod(self.id, .{ .basic_ack = .{
            .delivery_tag = delivery_tag,
            .multiple = multiple,
        } });
    }

    /// Acknowledge all deliveries up to and including this delivery tag.
    /// Equivalent to `ackUpTo`; kept for callers that prefer the AMQP-method-style name.
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
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => return error.ProtocolError,
        }
    }

    /// Wait until all published messages have been confirmed.
    /// Only useful when tracking is false; with tracking, each publish already waits.
    pub fn waitForConfirms(self: *Channel) !bool {
        return self.waitForConfirmsTimeout(self.connection.continuationTimeoutNs());
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
            if (!self.is_open.load(.acquire)) return self.typedClosedError();
            const elapsed = Io.Timestamp.now(io, .boot).nanoseconds - start.nanoseconds;
            if (elapsed >= limit) return error.Timeout;
            const remaining = limit - elapsed;
            const timeout: Io.Timeout = .{ .duration = .{ .raw = .{ .nanoseconds = remaining }, .clock = .awake } };
            const expected = self.confirm_signal.snapshot();
            self.confirm_mutex.unlock(io);
            self.confirm_signal.waitTimeout(io, expected, timeout);
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
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => return error.ProtocolError,
        }
    }

    /// Commit the current transaction.
    pub fn txCommit(self: *Channel) !void {
        try self.connection.sendMethod(self.id, .{ .tx_commit = {} });
        const response = try self.awaitMethod();
        switch (response) {
            .tx_commit_ok => {},
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => return error.ProtocolError,
        }
    }

    /// Rollback the current transaction.
    pub fn txRollback(self: *Channel) !void {
        try self.connection.sendMethod(self.id, .{ .tx_rollback = {} });
        const response = try self.awaitMethod();
        switch (response) {
            .tx_rollback_ok => {},
            .channel_close => |cc| return self.handleChannelClose(cc),
            else => return error.ProtocolError,
        }
    }

    //
    // Channel lifecycle
    //

    /// Close this channel gracefully, swallowing errors. Mirrors
    /// `Connection.close()` and is the form most callers want for
    /// `defer ch.close();`. Use `closeChannel()` when the caller needs
    /// the AMQP-level error code.
    pub fn close(self: *Channel) void {
        self.closeChannel() catch {};
    }

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

    /// Reset publisher-confirm sequence state so post-recovery publishes match
    /// the broker's restarted delivery-tag counter. Must be called only when
    /// the channel is closed and the reader thread is not delivering frames.
    pub fn resetConfirmStateAfterRecovery(self: *Channel) void {
        const io = getIo();
        self.confirm_mutex.lockUncancelable(io);
        defer self.confirm_mutex.unlock(io);
        self.next_publish_seq_no = 0;
        self.outstanding_count = 0;
        self.last_confirmed_seq = 0;
        self.confirm_promises.clearRetainingCapacity();
        self.confirm_signal.broadcast(io);
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

    /// Walk the in-flight map once and resolve any seq in [start, delivery_tag].
    /// Iteration cannot mutate the map, so collect keys first then remove. The
    /// stack batch sized at 128 covers virtually all real workloads in one pass.
    fn resolveSparseConfirms(self: *Channel, start: u64, delivery_tag: u64, result: ConfirmPromise.ConfirmResult, io: Io) u32 {
        var resolved_count: u32 = 0;
        var batch: [128]u64 = undefined;
        while (true) {
            var batch_pos: usize = 0;
            var it = self.confirm_promises.iterator();
            while (it.next()) |entry| {
                const seq = entry.key_ptr.*;
                if (seq >= start and seq <= delivery_tag) {
                    entry.value_ptr.*.result = result;
                    entry.value_ptr.*.event.set(io);
                    batch[batch_pos] = seq;
                    batch_pos += 1;
                    if (batch_pos == batch.len) break;
                }
            }
            for (batch[0..batch_pos]) |k| _ = self.confirm_promises.remove(k);
            resolved_count += @intCast(batch_pos);
            if (batch_pos < batch.len) break;
        }
        return resolved_count;
    }

    /// Process a publisher confirm from the server.
    fn handleConfirm(self: *Channel, delivery_tag: u64, multiple: bool, acked: bool) void {
        const io = getIo();
        self.confirm_mutex.lockUncancelable(io);

        const prev_confirmed = self.last_confirmed_seq;
        if (delivery_tag > self.last_confirmed_seq) {
            self.last_confirmed_seq = delivery_tag;
        }

        const start = if (multiple) prev_confirmed + 1 else delivery_tag;
        const result: ConfirmPromise.ConfirmResult = if (acked) .acked else .nacked;
        var resolved_count: u32 = 0;

        if (!multiple) {
            if (self.confirm_promises.fetchRemove(delivery_tag)) |kv| {
                kv.value.result = result;
                kv.value.event.set(io);
                resolved_count = 1;
            }
        } else {
            // Two strategies: walk the seq range, or walk the (smaller) map and
            // filter. Pick the cheaper one. The sparse case happens when many
            // single-acks have already drained the map but a wide multi-ack
            // arrives, e.g. recovery or a server that batches acks lazily.
            const map_count = self.confirm_promises.count();
            const range = delivery_tag - start + 1;
            if (map_count == 0) {
                // Nothing to do.
            } else if (range <= @as(u64, map_count) * 2) {
                var seq = start;
                while (seq <= delivery_tag) : (seq += 1) {
                    if (self.confirm_promises.fetchRemove(seq)) |kv| {
                        kv.value.result = result;
                        kv.value.event.set(io);
                        resolved_count += 1;
                    }
                }
            } else {
                resolved_count = self.resolveSparseConfirms(start, delivery_tag, result, io);
            }
        }

        if (self.confirm_tracking and self.outstanding_limit > 0) {
            // Saturating subtract would silently mask a tracking bug; log and clamp.
            if (resolved_count > self.outstanding_count) {
                log.err("confirm tracking inconsistency on channel {d}: resolved {d} promises but only {d} outstanding", .{ self.id, resolved_count, self.outstanding_count });
                self.outstanding_count = 0;
            } else {
                self.outstanding_count -= resolved_count;
            }
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
            .basic_ack => |ack_method| {
                self.handleConfirm(ack_method.delivery_tag, ack_method.multiple, true);
            },
            .basic_nack => |nack_method| {
                self.handleConfirm(nack_method.delivery_tag, nack_method.multiple, false);
            },
            .basic_cancel => |cancel| {
                log.info("consumer cancelled by server on channel {d}", .{self.id});
                if (self.consumers.fetchRemove(cancel.consumer_tag)) |kv| self.allocator.free(kv.key);
                if (self.on_cancel) |cb| cb(cancel.consumer_tag);
                self.event_listeners.emit(.{ .consumer_cancelled = cancel.consumer_tag });
            },
            .channel_close => |cc| {
                // Send close-ok immediately from the reader thread
                self.connection.sendMethod(self.id, .{ .channel_close_ok = {} }) catch {};
                self.connection.removeChannel(self.id);
                log.warn("channel {d} closed by server: [{d}] {s}", .{ self.id, cc.reply_code, cc.reply_text });
                self.recordCloseInfo(cc, true);
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
                .basic_deliver => |deliver| self.dispatchDelivery(deliver, header.properties, body),
                .basic_return => |ret| self.dispatchReturn(ret, header.properties, body),
                else => {},
            }
            self.pending_method = null;
        } else {
            self.deliverGetContent(header.properties, body);
        }

        self.pending_header = null;
        self.pending_body.clearRetainingCapacity();
    }

    fn dispatchDelivery(self: *Channel, deliver: anytype, props: BasicProperties, body: []const u8) void {
        const a = self.allocator;
        // Take ownership of pending_body to avoid a copy.
        const owned_body = self.pending_body.toOwnedSlice(a) catch
            a.dupe(u8, body) catch return;

        var delivery = buildOwnedDelivery(self, a, deliver, props, owned_body) catch {
            a.free(owned_body);
            log.err("failed to allocate delivery", .{});
            return;
        };

        if (self.consumers.get(delivery.consumer_tag)) |maybe_cb| {
            if (maybe_cb) |cb| {
                if (self.work_pool) |pool| {
                    pool.submit(cb, delivery);
                } else {
                    cb(delivery);
                    delivery.deinit(a);
                }
                return;
            }
            // Registered consumer without a callback: fall through and queue the
            // delivery for `recvDelivery`.
        }
        self.delivery_mutex.lockUncancelable(getIo());
        self.deliveries.append(a, delivery) catch {
            self.delivery_mutex.unlock(getIo());
            delivery.deinit(a);
            log.err("failed to enqueue delivery", .{});
            return;
        };
        self.delivery_signal.signal(getIo());
        self.delivery_mutex.unlock(getIo());
    }

    fn dispatchReturn(self: *Channel, ret: anytype, props: BasicProperties, body: []const u8) void {
        const cb = self.on_return orelse return;
        const a = self.allocator;
        var returned = buildOwnedReturn(a, ret, props, body) catch {
            log.err("failed to allocate returned message", .{});
            return;
        };
        cb(returned);
        returned.deinit(a);
    }

    fn deliverGetContent(self: *Channel, props: BasicProperties, body: []const u8) void {
        const a = self.allocator;
        const owned_props = props.deepCopy(a) catch BasicProperties.default;
        self.rpc_mutex.lockUncancelable(getIo());
        self.get_properties.deinitOwned(a);
        self.get_properties = owned_props;
        self.get_body.clearRetainingCapacity();
        self.get_body.appendSlice(a, body) catch {};
        self.get_ready = true;
        self.rpc_signal.signal(getIo());
        self.rpc_mutex.unlock(getIo());
    }

    fn buildOwnedDelivery(channel: *Channel, a: Allocator, deliver: anytype, props: BasicProperties, body: []const u8) !Delivery {
        const consumer_tag = try a.dupe(u8, deliver.consumer_tag);
        errdefer a.free(consumer_tag);
        const exchange = try a.dupe(u8, deliver.exchange);
        errdefer a.free(exchange);
        const routing_key = try a.dupe(u8, deliver.routing_key);
        errdefer a.free(routing_key);
        var owned_props = try props.deepCopy(a);
        errdefer owned_props.deinitOwned(a);
        return .{
            .channel = channel,
            .consumer_tag = consumer_tag,
            .delivery_tag = deliver.delivery_tag,
            .redelivered = deliver.redelivered,
            .exchange = exchange,
            .routing_key = routing_key,
            .properties = owned_props,
            .body = body,
        };
    }

    fn buildOwnedReturn(a: Allocator, ret: anytype, props: BasicProperties, body: []const u8) !ReturnedMessage {
        const reply_text = try a.dupe(u8, ret.reply_text);
        errdefer a.free(reply_text);
        const exchange = try a.dupe(u8, ret.exchange);
        errdefer a.free(exchange);
        const routing_key = try a.dupe(u8, ret.routing_key);
        errdefer a.free(routing_key);
        var owned_props = try props.deepCopy(a);
        errdefer owned_props.deinitOwned(a);
        const owned_body = try a.dupe(u8, body);
        errdefer a.free(owned_body);
        return .{
            .reply_code = ret.reply_code,
            .reply_text = reply_text,
            .exchange = exchange,
            .routing_key = routing_key,
            .properties = owned_props,
            .body = owned_body,
        };
    }

    /// Wait for the next RPC response (used by synchronous operations).
    pub fn awaitMethod(self: *Channel) !Method {
        return self.awaitMethodTimeout(self.connection.continuationTimeoutNs());
    }

    fn awaitMethodTimeout(self: *Channel, timeout_ns: u64) !Method {
        const io = getIo();
        const start = Io.Timestamp.now(io, .boot);
        const limit: i96 = @intCast(timeout_ns);

        self.rpc_mutex.lockUncancelable(io);
        defer self.rpc_mutex.unlock(io);

        while (self.rpc_response == null) {
            if (!self.is_open.load(.acquire)) return self.typedClosedError();
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

    const ContentResult = struct {
        properties: BasicProperties,
        body: []const u8,
    };

    /// Wait for header + body, then transfer ownership of the assembled
    /// properties and body out of the channel's get_* state to the caller.
    fn takeOwnedGetContent(self: *Channel) !ContentResult {
        const io = getIo();
        const timeout_ns = self.connection.continuationTimeoutNs();
        const start = Io.Timestamp.now(io, .boot);
        const limit: i96 = @intCast(timeout_ns);

        self.rpc_mutex.lockUncancelable(io);
        defer self.rpc_mutex.unlock(io);

        while (!self.get_ready) {
            if (!self.is_open.load(.acquire)) return self.typedClosedError();
            const elapsed = Io.Timestamp.now(io, .boot).nanoseconds - start.nanoseconds;
            if (elapsed >= limit) return error.Timeout;
            const remaining = limit - elapsed;
            const timeout: Io.Timeout = .{ .duration = .{ .raw = .{ .nanoseconds = remaining }, .clock = .awake } };
            const expected = self.rpc_signal.snapshot();
            self.rpc_mutex.unlock(io);
            self.rpc_signal.waitTimeout(io, expected, timeout);
            self.rpc_mutex.lockUncancelable(io);
        }

        const props = self.get_properties;
        self.get_properties = BasicProperties.default;
        const body = self.get_body.toOwnedSlice(self.allocator) catch &[_]u8{};
        self.get_ready = false;
        return .{ .properties = props, .body = body };
    }

    /// Called by RPC waiters that received channel_close as their response.
    /// The reader thread already sent close-ok, recorded the close info, emitted
    /// the event, and called closeInternal. Map the reply code to a typed error.
    fn handleChannelClose(_: *Channel, cc: method_mod.ChannelClose) ChannelError {
        return replyCodeToError(cc.reply_code);
    }

    /// Record a server-initiated close so applications can inspect details
    /// after a typed error returns. Replaces any prior recorded close.
    fn recordCloseInfo(self: *Channel, cc: method_mod.ChannelClose, initiated_by_server: bool) void {
        const text = self.allocator.dupe(u8, cc.reply_text) catch &[_]u8{};
        const io = getIo();
        self.close_info_mutex.lockUncancelable(io);
        defer self.close_info_mutex.unlock(io);
        if (self.last_close) |prev| prev.deinit(self.allocator);
        self.last_close = .{
            .reply_code = cc.reply_code,
            .reply_text = text,
            .class_id = cc.class_id,
            .method_id = cc.method_id,
            .initiated_by_server = initiated_by_server,
        };
    }

    /// Most recent server-initiated channel.close, or null. The struct's
    /// scalars are copies; `reply_text` borrows from the channel and stays
    /// valid for the channel's lifetime.
    pub fn lastClose(self: *Channel) ?ChannelCloseInfo {
        const io = getIo();
        self.close_info_mutex.lockUncancelable(io);
        defer self.close_info_mutex.unlock(io);
        return self.last_close;
    }

    /// Map the most recent server close to a typed error, falling back to
    /// `error.ChannelClosed` when the channel was closed by the application
    /// itself or by the underlying transport.
    fn typedClosedError(self: *Channel) ChannelError {
        if (self.lastClose()) |info| return replyCodeToError(info.reply_code);
        return error.ChannelClosed;
    }
};
