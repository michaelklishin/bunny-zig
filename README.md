# bunny-zig

A Zig client library for [RabbitMQ](https://rabbitmq.com) that implements AMQP 0-9-1.

Heavily inspired by [Ruby Bunny](https://github.com/ruby-amqp/bunny),
[`bunny-swift`](https://github.com/michaelklishin/bunny-swift),
and [`bunny-rs`](https://github.com/michaelklishin/bunny-rs) (Rust).


## Supported RabbitMQ Versions

 * RabbitMQ 4.x
 * RabbitMQ 3.13.x


## Platform Support

 * Zig 0.16.0 or later
 * Linux (x86_64, aarch64)
 * macOS (Apple Silicon, Intel)


## Project Maturity

This is a young project. Breaking API changes are possible between minor versions.


## Installation

Add bunny-zig as a dependency in your `build.zig.zon`:

```zig
.dependencies = .{
    .bunny = .{
        .url = "https://github.com/michaelklishin/bunny-zig/archive/refs/tags/0.1.0.tar.gz",
        .hash = "...",
    },
},
```

Then in your `build.zig`:

```zig
const bunny_dep = b.dependency("bunny", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("bunny", bunny_dep.module("bunny"));
```


## Quick Start

```zig
const std = @import("std");
const bunny = @import("bunny");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const conn = try bunny.Connection.open(allocator, .{});
    defer conn.deinit();

    const ch = try conn.openChannel();
    defer ch.closeChannel() catch {};

    const q = try ch.declareQueueHandle("hello", .{ .auto_delete = true });

    try ch.confirmSelect();
    try q.publish("Hello, Zig world!", .{});
    _ = try ch.waitForConfirms();

    _ = try q.subscribe(.manual);
    if (try ch.recvDelivery()) |raw| {
        var msg = raw;
        defer msg.deinit(allocator);
        std.debug.print("Received: {s}\n", .{msg.body});
        try ch.basicAck(msg.delivery_tag, false);
    }
}
```


## Usage Examples

### Connecting

```zig
// Defaults: localhost:5672, guest/guest, and the "/" virtual host
const conn = try bunny.Connection.open(allocator, .{});

// Custom options
const conn2 = try bunny.Connection.open(allocator, .{
    .host = "rabbitmq.example.com",
    .port = 5672,
    .username = "myapp",
    .password = "secret",
    .virtual_host = "/production",
    .heartbeat = 30,
    .connection_name = "my-app",
});

// Multiple hosts for redundancy
const conn3 = try bunny.Connection.open(allocator, .{
    .host = "rmq1.example.com",
    .hosts = &.{
        .{ .host = "rmq2.example.com", .port = 5672 },
        .{ .host = "rmq3.example.com", .port = 5672 },
    },
});

// From an AMQP URI
const conn4 = try bunny.Connection.openUri(allocator, "amqp://user:pass@host:5672/vhost");
```

### Connection Recovery

```zig
const conn = try bunny.Connection.open(allocator, .{
    .recovery = .{
        .enabled = true,
        .initial_interval_ms = 5_000,
        .max_interval_ms = 60_000,
        .backoff_multiplier = 2.0,
        .max_attempts = 10,
    },
});

// Listen for recovery events
try conn.addEventListener(&struct {
    fn handler(event: bunny.ConnectionEvent) void {
        switch (event) {
            .recovery_started => std.debug.print("reconnecting...\n", .{}),
            .recovery_succeeded => std.debug.print("recovered\n", .{}),
            .recovery_failed => |reason| std.debug.print("recovery failed: {s}\n", .{reason}),
            else => {},
        }
    }
}.handler);
```

### TLS

```zig
// AMQPS via URI (default port 5671)
const conn = try bunny.Connection.openUri(allocator, "amqps://user:pass@rmq.example.com/vhost");

// Explicit TLS options with a custom CA bundle
const conn2 = try bunny.Connection.open(allocator, .{
    .host = "rmq.example.com",
    .port = 5671,
    .tls = .{
        .host = "rmq.example.com",
        .ca_file = "/etc/rabbitmq/ssl/ca_certificate.pem",
    },
});

// Test-only: skip chain verification. DO NOT use in production.
const conn3 = try bunny.Connection.open(allocator, .{
    .host = "localhost",
    .port = 5671,
    .tls = .{
        .host = "localhost",
        .skip_peer_certificate_chain_verification = true,
    },
});
```

### Queue Types

```zig
// Classic queue (default)
_ = try ch.queueDeclare("my.classic.queue", .{ .durable = true });

// Quorum queue (replicated, durable)
_ = try ch.quorumQueue("my.quorum.queue");

// Stream queue (append-only log)
_ = try ch.streamQueue("my.stream.queue");

// Temporary queue (server-named, transient, exclusive)
const tmp = try ch.temporaryQueue();
std.debug.print("Temporary queue: {s}\n", .{tmp.name});
```

### Stream Queues

```zig
_ = try ch.streamQueue("events");

// Consume from the beginning of the stream
const args = try bunny.FieldTable.fromEntries(allocator, &.{
    .{ .key = "x-stream-offset", .value = .{ .long_string = "first" } },
});
defer args.deinit();

try ch.basicQos(100, false);
_ = try ch.basicConsumeWithArgs("events", "", .manual, false, args);
```

### Queue Arguments Builder

```zig
var args = bunny.QueueArguments{};
_ = try args.messageTtl(allocator, 60_000);
_ = try args.maxLength(allocator, 10_000);
_ = try args.deadLetterExchange(allocator, "dlx");
_ = try args.overflow(allocator, .reject_publish);
var table = try args.build(allocator);
defer table.deinit();

_ = try ch.queueDeclare("my.configured.queue", .{
    .durable = true,
    .arguments = table,
});
```

### Tanzu RabbitMQ Queue Types

```zig
// Delayed queue with retry on failed deliveries
var dq_args = bunny.QueueArguments{};
_ = try dq_args.delayedRetryType(allocator, .failed);
_ = try dq_args.delayedRetryMin(allocator, 1_000);
_ = try dq_args.delayedRetryMax(allocator, 60_000);
var dq_table = try dq_args.build(allocator);
defer dq_table.deinit();
_ = try ch.delayedQueue("tasks.retry", .{ .arguments = dq_table });

// JMS queue with selector support
var jms_args = bunny.QueueArguments{};
const fields = [_][]const u8{ "priority", "region" };
_ = try jms_args.selectorFields(allocator, &fields);
var jms_table = try jms_args.build(allocator);
defer jms_table.deinit();
_ = try ch.jmsQueue("orders", .{ .arguments = jms_table });

// Consume with a JMS selector
_ = try ch.basicConsumeJms("orders", "my-consumer", .manual,
    "priority > 5 AND region = 'EU'");
```

### Publishing Messages

```zig
// Simple publish to default exchange
try ch.publishToQueue("my-queue", "Hello!", .{});

// Publish with properties
try ch.publish("payload", .{
    .exchange = "my-exchange",
    .routing_key = "orders.new",
    .mandatory = true,
    .properties = bunny.BasicProperties.default
        .withContentType("application/json")
        .asPersistent()
        .withMessageId("order-123")
        .withCorrelationId("req-456")
        .withAppId("order-service"),
});
```

### Publisher Confirms

bunny-zig supports three confirm modes, matching the bunny-swift design:

**Batch mode**: publish many, then wait:

```zig
try ch.confirmSelect();

for (0..100) |i| {
    var buf: [32]u8 = undefined;
    const msg = try std.fmt.bufPrint(&buf, "message {d}", .{i});
    try ch.publishToQueue("my-queue", msg, .{});
}

const all_confirmed = try ch.waitForConfirms();
```

**Per-message tracking**: each publish blocks until the broker confirms:

```zig
try ch.confirmSelectWithOptions(.{ .tracking = true });

// Each call blocks until the broker acks. Returns error.PublishNacked on nack.
try ch.publishToQueue("my-queue", "message 1", .{});
try ch.publishToQueue("my-queue", "message 2", .{});
```

**Per-message tracking with backpressure**: limits unconfirmed messages:

```zig
try ch.confirmSelectWithOptions(.{ .tracking = true, .outstanding_limit = 128 });

// Publish blocks if 128 messages are already unconfirmed, resuming
// as the broker confirms earlier messages.
for (0..10_000) |i| {
    var buf: [32]u8 = undefined;
    const msg = try std.fmt.bufPrint(&buf, "message {d}", .{i});
    try ch.publishToQueue("my-queue", msg, .{});
}
```

### Consuming Messages

```zig
// global=false: per-consumer prefetch (each consumer gets its own window)
// global=true:  channel-wide prefetch shared across all consumers
try ch.basicQos(10, false);

// Start consuming
_ = try ch.basicConsume("my-queue", "my-consumer", .manual);

// Receive deliveries (blocks until a message arrives)
while (try ch.recvDelivery()) |delivery| {
    std.debug.print("Received: {s}\n", .{delivery.bodyString()});
    try ch.basicAck(delivery.delivery_tag, false);
}
```

### Callback-based Consumers

```zig
_ = try ch.basicConsumeWith("my-queue", "my-consumer", .manual, &struct {
    fn handler(delivery: bunny.Delivery) void {
        std.debug.print("Got: {s}\n", .{delivery.body});
    }
}.handler);
```

### Polling (basic.get)

```zig
if (try ch.basicGet("my-queue", .manual)) |msg| {
    std.debug.print("Got: {s}\n", .{msg.body});
    try ch.basicAck(msg.delivery_tag, false);
} else {
    std.debug.print("Queue is empty\n", .{});
}
```

### Rejecting and Negative Acknowledgements

```zig
// Reject a single message, requeue it
try ch.basicReject(delivery.delivery_tag, true);

// Nack with optional multiple and requeue
try ch.basicNack(delivery.delivery_tag, false, true);
```

### Exchange Operations

```zig
// Declare exchanges
try ch.declareDirect("my.direct");
try ch.declareFanout("my.fanout");
try ch.declareTopic("my.topic");
try ch.declareHeaders("my.headers");

// Custom exchange with options
try ch.exchangeDeclare("my.custom", "x-consistent-hash", .{
    .durable = true,
    .auto_delete = false,
});

// Delete an exchange
try ch.exchangeDelete("my.exchange");

// Exchange-to-exchange binding (RabbitMQ extension)
try ch.exchangeBind("destination", "source", "routing.key");
try ch.exchangeUnbind("destination", "source", "routing.key");
```

### Queue and Exchange Handles

```zig
// Declare and get a handle for convenient operations
const q = try ch.declareQueueHandle("my-queue", .{ .durable = true });
try q.publish("hello", .{});
try q.bind("my-exchange", "routing.key");
_ = try q.purge();

const ex = try ch.declareExchangeHandle("my-exchange", "topic", .{ .durable = true });
try ex.publish("payload", "routing.key", .{});
```

### Queue Binding

```zig
try ch.queueBind("my-queue", "my-exchange", "routing.key.*");
try ch.queueUnbind("my-queue", "my-exchange", "routing.key.*");
```

### Headers Exchange

```zig
try ch.declareHeaders("orders.headers");
_ = try ch.queueDeclare("orders.eu", .{ .auto_delete = true });

// Bind with x-match=all so every header pair must match
const bind_args = try bunny.FieldTable.fromEntries(allocator, &.{
    .{ .key = "x-match", .value = .{ .long_string = "all" } },
    .{ .key = "region", .value = .{ .long_string = "eu" } },
    .{ .key = "priority", .value = .{ .i32 = 5 } },
});
defer bind_args.deinit();
try ch.queueBindWithArgs("orders.eu", "orders.headers", "", bind_args);

// Publish with headers that match
const headers = try bunny.FieldTable.fromEntries(allocator, &.{
    .{ .key = "region", .value = .{ .long_string = "eu" } },
    .{ .key = "priority", .value = .{ .i32 = 5 } },
});
defer headers.deinit();
try ch.publish("payload", .{
    .exchange = "orders.headers",
    .properties = bunny.BasicProperties.default.withHeaders(headers),
});
```

### Mandatory Publish and Returned Messages

```zig
ch.on_return = &struct {
    fn handler(msg: bunny.ReturnedMessage) void {
        std.debug.print("returned [{d}] {s} from {s}\n", .{ msg.reply_code, msg.reply_text, msg.exchange });
    }
}.handler;

try ch.confirmSelect();
try ch.publish("won't route", .{
    .exchange = "amq.direct",
    .routing_key = "no.such.binding",
    .mandatory = true,
});
_ = try ch.waitForConfirms();
```

### Typed Channel Errors

```zig
const result = ch.queueDeclarePassive("might.not.exist");
result catch |err| switch (err) {
    error.NotFound => {
        // Channel is closed; inspect the broker's reply text and offending method.
        if (ch.lastClose()) |info| {
            std.debug.print("404 on class {d} method {d}: {s}\n", .{ info.class_id, info.method_id, info.reply_text });
        }
    },
    error.AccessRefused, error.PreconditionFailed, error.ResourceLocked => return err,
    else => return err,
};
```

### [`connection.blocked`](https://www.rabbitmq.com/docs/connection-blocked) Notifications

```zig
const conn = try bunny.Connection.open(allocator, .{ .host = "localhost" });
defer conn.deinit();

// Fired when the broker pauses publishers due to memory or disk pressure.
try conn.addEventListener(&struct {
    fn handler(event: bunny.ConnectionEvent) void {
        switch (event) {
            .blocked => |reason| std.debug.print("blocked: {s}\n", .{reason}),
            .unblocked => std.debug.print("unblocked\n", .{}),
            else => {},
        }
    }
}.handler);
```

### Transactions

```zig
try ch.txSelect();
try ch.publish("msg1", .{ .routing_key = "q1" });
try ch.publish("msg2", .{ .routing_key = "q2" });
try ch.txCommit(); // or ch.txRollback()
```


## Building and Testing

```bash
# Install Zig (macOS)
brew install zig

# Build
zig build

# Run unit tests (no RabbitMQ required)
zig build test

# Run integration tests (requires RabbitMQ on localhost:5672)
zig build integration-test
```


## Documentation

 * [RabbitMQ Tutorials](https://www.rabbitmq.com/tutorials)
 * [AMQP 0-9-1 Reference](https://www.rabbitmq.com/amqp-0-9-1-reference)
 * [RabbitMQ Documentation](https://www.rabbitmq.com/docs)


## Community

 * [GitHub Discussions](https://github.com/michaelklishin/bunny-zig/discussions)
 * [RabbitMQ Discord](https://rabbitmq.com/discord)
 * [RabbitMQ Mailing List](https://groups.google.com/forum/#!forum/rabbitmq-users)


## License

Dual licensed under Apache License 2.0 and MIT. See [LICENSE-APACHE2](LICENSE-APACHE2) and [LICENSE-MIT](LICENSE-MIT).
