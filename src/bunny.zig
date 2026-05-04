/// bunny-zig: A Zig client library for RabbitMQ.
///
/// Implements AMQP 0-9-1 with support for publisher confirms, consumer
/// acknowledgements, exchange-to-exchange bindings, and all standard
/// queue types (classic, quorum, stream).

pub const protocol = @import("protocol.zig");
pub const Connection = @import("connection.zig").Connection;
pub const ConnectionOptions = @import("connection.zig").ConnectionOptions;
pub const ConnectionError = @import("connection.zig").ConnectionError;
pub const ConnectError = @import("connection.zig").ConnectError;
pub const HandshakeError = @import("connection.zig").HandshakeError;
pub const ChannelOpenError = @import("connection.zig").ChannelOpenError;
pub const ChannelError = @import("connection.zig").ChannelError;
pub const UriError = @import("connection.zig").UriError;
pub const Endpoint = @import("connection.zig").Endpoint;
pub const AddressResolver = @import("connection.zig").AddressResolver;
pub const Channel = @import("channel.zig").Channel;
pub const Delivery = @import("channel.zig").Delivery;
pub const GetResult = @import("channel.zig").GetResult;
pub const ReturnedMessage = @import("channel.zig").ReturnedMessage;
pub const ChannelCloseInfo = @import("channel.zig").ChannelCloseInfo;
pub const replyCodeToError = @import("channel.zig").replyCodeToError;
pub const AckMode = @import("channel.zig").AckMode;
pub const ExchangeType = @import("channel.zig").ExchangeType;
pub const QueueType = @import("channel.zig").QueueType;
pub const QueueDeclareOptions = @import("channel.zig").QueueDeclareOptions;
pub const ExchangeDeclareOptions = @import("channel.zig").ExchangeDeclareOptions;
pub const PublishOptions = @import("channel.zig").PublishOptions;
pub const FlushStrategy = @import("channel.zig").FlushStrategy;
pub const ConfirmSelectOptions = @import("channel.zig").Channel.ConfirmSelectOptions;
pub const ConfirmPromise = @import("channel.zig").ConfirmPromise;
pub const Transport = @import("transport.zig").Transport;
pub const TlsOptions = @import("transport.zig").TlsOptions;
pub const Queue = @import("queue.zig").Queue;
pub const Exchange = @import("exchange.zig").Exchange;
pub const ConnectionEvent = @import("events.zig").ConnectionEvent;
pub const ChannelEvent = @import("events.zig").ChannelEvent;
pub const ConsumerWorkPool = @import("consumer_work_pool.zig").ConsumerWorkPool;
pub const RecoveryConfig = @import("recovery.zig").RecoveryConfig;
pub const TopologyRegistry = @import("recovery.zig").TopologyRegistry;
pub const QueueArguments = @import("x_arguments.zig").QueueArguments;
pub const OverflowStrategy = @import("x_arguments.zig").OverflowStrategy;
pub const DeadLetterStrategy = @import("x_arguments.zig").DeadLetterStrategy;
pub const QueueLeaderLocator = @import("x_arguments.zig").QueueLeaderLocator;
pub const DelayedRetryType = @import("x_arguments.zig").DelayedRetryType;

// Re-export commonly used protocol types
pub const BasicProperties = protocol.BasicProperties;
pub const FieldTable = protocol.FieldTable;
pub const FieldValue = protocol.FieldValue;

/// Length of a correlation ID written by `newCorrelationId`.
pub const correlation_id_len: usize = 16;

/// Generate a random 16-character hex correlation ID by value. Useful for
/// RPC clients that need a unique tag to match responses.
pub fn correlationId(io: @import("std").Io) [correlation_id_len]u8 {
    const std = @import("std");
    var bytes: [8]u8 = undefined;
    var src = std.Random.IoSource{ .io = io };
    src.interface().bytes(&bytes);
    var out: [correlation_id_len]u8 = undefined;
    _ = std.fmt.bufPrint(&out, "{x}", .{bytes}) catch unreachable;
    return out;
}

/// Write a random 16-character hex correlation ID into `buf` and return the
/// slice. Returns `error.BufferTooSmall` if `buf.len < 16`.
pub fn newCorrelationId(io: @import("std").Io, buf: []u8) error{BufferTooSmall}![]const u8 {
    if (buf.len < correlation_id_len) return error.BufferTooSmall;
    const id = correlationId(io);
    @memcpy(buf[0..correlation_id_len], &id);
    return buf[0..correlation_id_len];
}

test {
    const std = @import("std");
    std.testing.refAllDecls(@This());
}

test "newCorrelationId fills buffer with 16 hex chars" {
    const std = @import("std");
    const io = std.Io.Threaded.global_single_threaded.io();
    var buf: [16]u8 = undefined;
    const id = try newCorrelationId(io, &buf);
    try std.testing.expectEqual(@as(usize, 16), id.len);
    for (id) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}

test "newCorrelationId produces distinct IDs across calls" {
    const std = @import("std");
    const io = std.Io.Threaded.global_single_threaded.io();
    var a: [16]u8 = undefined;
    var b: [16]u8 = undefined;
    const id_a = try newCorrelationId(io, &a);
    const id_b = try newCorrelationId(io, &b);
    try std.testing.expect(!std.mem.eql(u8, id_a, id_b));
}

test "newCorrelationId returns BufferTooSmall when buf is short" {
    const std = @import("std");
    const io = std.Io.Threaded.global_single_threaded.io();
    var buf: [8]u8 = undefined;
    try std.testing.expectError(error.BufferTooSmall, newCorrelationId(io, &buf));
}

test "correlationId by-value variant returns 16 hex chars" {
    const std = @import("std");
    const io = std.Io.Threaded.global_single_threaded.io();
    const id = correlationId(io);
    try std.testing.expectEqual(@as(usize, 16), id.len);
    for (id) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}
