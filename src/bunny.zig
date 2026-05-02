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
pub const QueueInfo = @import("channel.zig").QueueInfo;
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

test {
    const std = @import("std");
    std.testing.refAllDecls(@This());
}
