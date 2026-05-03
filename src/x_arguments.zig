/// Builder helpers for well-known RabbitMQ x-arguments on queues and exchanges.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("protocol.zig").types;
const FieldTable = types.FieldTable;
const FieldValue = types.FieldValue;

pub const OverflowStrategy = enum {
    drop_head,
    reject_publish,
    reject_publish_dlx,

    pub fn toSlice(self: OverflowStrategy) []const u8 {
        return switch (self) {
            .drop_head => "drop-head",
            .reject_publish => "reject-publish",
            .reject_publish_dlx => "reject-publish-dlx",
        };
    }
};

pub const DeadLetterStrategy = enum {
    at_most_once,
    at_least_once,

    pub fn toSlice(self: DeadLetterStrategy) []const u8 {
        return switch (self) {
            .at_most_once => "at-most-once",
            .at_least_once => "at-least-once",
        };
    }
};

pub const QueueLeaderLocator = enum {
    client_local,
    balanced,

    pub fn toSlice(self: QueueLeaderLocator) []const u8 {
        return switch (self) {
            .client_local => "client-local",
            .balanced => "balanced",
        };
    }
};

/// Tanzu RabbitMQ delayed queue retry type.
pub const DelayedRetryType = enum {
    all,
    failed,
    returned,

    pub fn toSlice(self: DelayedRetryType) []const u8 {
        return switch (self) {
            .all => "all",
            .failed => "failed",
            .returned => "returned",
        };
    }
};

/// Accumulates x-arguments for queue declaration.
pub const QueueArguments = struct {
    entries: std.ArrayList(FieldTable.Entry) = .empty,

    pub fn build(self: *QueueArguments, allocator: Allocator) !FieldTable {
        return FieldTable{
            .entries = try self.entries.toOwnedSlice(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *QueueArguments, allocator: Allocator) void {
        self.entries.deinit(allocator);
    }

    fn append(self: *QueueArguments, allocator: Allocator, key: []const u8, value: FieldValue) !void {
        try self.entries.append(allocator, .{ .key = key, .value = value });
    }

    pub fn queueType(self: *QueueArguments, allocator: Allocator, queue_type: []const u8) !*QueueArguments {
        try self.append(allocator, "x-queue-type", .{ .long_string = queue_type });
        return self;
    }

    pub fn messageTtl(self: *QueueArguments, allocator: Allocator, ms: i64) !*QueueArguments {
        try self.append(allocator, "x-message-ttl", .{ .i64 = ms });
        return self;
    }

    pub fn maxLength(self: *QueueArguments, allocator: Allocator, n: i64) !*QueueArguments {
        try self.append(allocator, "x-max-length", .{ .i64 = n });
        return self;
    }

    pub fn maxLengthBytes(self: *QueueArguments, allocator: Allocator, n: i64) !*QueueArguments {
        try self.append(allocator, "x-max-length-bytes", .{ .i64 = n });
        return self;
    }

    pub fn deadLetterExchange(self: *QueueArguments, allocator: Allocator, name: []const u8) !*QueueArguments {
        try self.append(allocator, "x-dead-letter-exchange", .{ .long_string = name });
        return self;
    }

    pub fn deadLetterRoutingKey(self: *QueueArguments, allocator: Allocator, key: []const u8) !*QueueArguments {
        try self.append(allocator, "x-dead-letter-routing-key", .{ .long_string = key });
        return self;
    }

    pub fn deadLetterStrategy(self: *QueueArguments, allocator: Allocator, strategy: DeadLetterStrategy) !*QueueArguments {
        try self.append(allocator, "x-dead-letter-strategy", .{ .long_string = strategy.toSlice() });
        return self;
    }

    pub fn overflow(self: *QueueArguments, allocator: Allocator, strategy: OverflowStrategy) !*QueueArguments {
        try self.append(allocator, "x-overflow", .{ .long_string = strategy.toSlice() });
        return self;
    }

    pub fn singleActiveConsumer(self: *QueueArguments, allocator: Allocator) !*QueueArguments {
        try self.append(allocator, "x-single-active-consumer", .{ .boolean = true });
        return self;
    }

    pub fn maxPriority(self: *QueueArguments, allocator: Allocator, n: u8) !*QueueArguments {
        try self.append(allocator, "x-max-priority", .{ .u8 = n });
        return self;
    }

    pub fn expires(self: *QueueArguments, allocator: Allocator, ms: i64) !*QueueArguments {
        try self.append(allocator, "x-expires", .{ .i64 = ms });
        return self;
    }

    pub fn queueLeaderLocator(self: *QueueArguments, allocator: Allocator, locator: QueueLeaderLocator) !*QueueArguments {
        try self.append(allocator, "x-queue-leader-locator", .{ .long_string = locator.toSlice() });
        return self;
    }

    pub fn deliveryLimit(self: *QueueArguments, allocator: Allocator, n: i64) !*QueueArguments {
        try self.append(allocator, "x-delivery-limit", .{ .i64 = n });
        return self;
    }

    pub fn queueMode(self: *QueueArguments, allocator: Allocator, mode: []const u8) !*QueueArguments {
        try self.append(allocator, "x-queue-mode", .{ .long_string = mode });
        return self;
    }

    // Tanzu RabbitMQ: delayed queue arguments

    pub fn delayedRetryType(self: *QueueArguments, allocator: Allocator, retry_type: DelayedRetryType) !*QueueArguments {
        try self.append(allocator, "x-delayed-retry-type", .{ .long_string = retry_type.toSlice() });
        return self;
    }

    pub fn delayedRetryMin(self: *QueueArguments, allocator: Allocator, ms: i64) !*QueueArguments {
        try self.append(allocator, "x-delayed-retry-min", .{ .i64 = ms });
        return self;
    }

    pub fn delayedRetryMax(self: *QueueArguments, allocator: Allocator, ms: i64) !*QueueArguments {
        try self.append(allocator, "x-delayed-retry-max", .{ .i64 = ms });
        return self;
    }

    pub fn consumerDisconnectedTimeout(self: *QueueArguments, allocator: Allocator, ms: i64) !*QueueArguments {
        try self.append(allocator, "x-consumer-disconnected-timeout", .{ .i64 = ms });
        return self;
    }

    // Tanzu RabbitMQ: JMS queue arguments

    pub fn selectorFields(self: *QueueArguments, allocator: Allocator, fields: []const []const u8) !*QueueArguments {
        const items = try allocator.alloc(FieldValue, fields.len);
        for (fields, 0..) |field, i| {
            items[i] = .{ .long_string = field };
        }
        try self.append(allocator, "x-selector-fields", .{ .array = items });
        return self;
    }

    pub fn selectorFieldMaxBytes(self: *QueueArguments, allocator: Allocator, n: i64) !*QueueArguments {
        try self.append(allocator, "x-selector-field-max-bytes", .{ .i64 = n });
        return self;
    }
};

// Tests

test "queue arguments: message TTL and dead letter exchange" {
    const allocator = std.testing.allocator;
    var args = QueueArguments{};
    _ = try args.messageTtl(allocator, 60000);
    _ = try args.deadLetterExchange(allocator, "dlx");
    _ = try args.deadLetterRoutingKey(allocator, "dlq");

    var table = try args.build(allocator);
    defer table.deinit();

    try std.testing.expectEqual(3, table.entries.len);
    try std.testing.expectEqual(60000, table.get("x-message-ttl").?.i64);
    try std.testing.expectEqualSlices(u8, "dlx", table.get("x-dead-letter-exchange").?.long_string);
    try std.testing.expectEqualSlices(u8, "dlq", table.get("x-dead-letter-routing-key").?.long_string);
}

test "queue arguments: max length and overflow" {
    const allocator = std.testing.allocator;
    var args = QueueArguments{};
    _ = try args.maxLength(allocator, 1000);
    _ = try args.overflow(allocator, .reject_publish);

    var table = try args.build(allocator);
    defer table.deinit();

    try std.testing.expectEqual(1000, table.get("x-max-length").?.i64);
    try std.testing.expectEqualSlices(u8, "reject-publish", table.get("x-overflow").?.long_string);
}

test "queue arguments: priority queue" {
    const allocator = std.testing.allocator;
    var args = QueueArguments{};
    _ = try args.maxPriority(allocator, 10);

    var table = try args.build(allocator);
    defer table.deinit();

    try std.testing.expectEqual(10, table.get("x-max-priority").?.u8);
}

test "queue arguments: single active consumer" {
    const allocator = std.testing.allocator;
    var args = QueueArguments{};
    _ = try args.singleActiveConsumer(allocator);

    var table = try args.build(allocator);
    defer table.deinit();

    try std.testing.expect(table.get("x-single-active-consumer").?.boolean);
}

test "queue arguments: quorum queue with delivery limit" {
    const allocator = std.testing.allocator;
    var args = QueueArguments{};
    _ = try args.queueType(allocator, "quorum");
    _ = try args.deliveryLimit(allocator, 5);
    _ = try args.queueLeaderLocator(allocator, .balanced);

    var table = try args.build(allocator);
    defer table.deinit();

    try std.testing.expectEqualSlices(u8, "quorum", table.get("x-queue-type").?.long_string);
    try std.testing.expectEqual(5, table.get("x-delivery-limit").?.i64);
    try std.testing.expectEqualSlices(u8, "balanced", table.get("x-queue-leader-locator").?.long_string);
}

test "queue arguments: empty builds empty table" {
    const allocator = std.testing.allocator;
    var args = QueueArguments{};

    var table = try args.build(allocator);
    defer table.deinit();

    try std.testing.expectEqual(0, table.entries.len);
}

test "queue arguments: chaining" {
    const allocator = std.testing.allocator;
    var args = QueueArguments{};
    _ = try (try (try args.messageTtl(allocator, 5000)).maxLength(allocator, 100)).overflow(allocator, .drop_head);

    var table = try args.build(allocator);
    defer table.deinit();

    try std.testing.expectEqual(3, table.entries.len);
}

test "queue arguments: delayed queue with retry" {
    const allocator = std.testing.allocator;
    var args = QueueArguments{};
    _ = try args.delayedRetryType(allocator, .failed);
    _ = try args.delayedRetryMin(allocator, 1000);
    _ = try args.delayedRetryMax(allocator, 60000);
    _ = try args.consumerDisconnectedTimeout(allocator, 30000);

    var table = try args.build(allocator);
    defer table.deinit();

    try std.testing.expectEqual(4, table.entries.len);
    try std.testing.expectEqualSlices(u8, "failed", table.get("x-delayed-retry-type").?.long_string);
    try std.testing.expectEqual(1000, table.get("x-delayed-retry-min").?.i64);
    try std.testing.expectEqual(60000, table.get("x-delayed-retry-max").?.i64);
    try std.testing.expectEqual(30000, table.get("x-consumer-disconnected-timeout").?.i64);
}

test "queue arguments: JMS selector fields as AMQP array" {
    const allocator = std.testing.allocator;
    var args = QueueArguments{};
    const fields = [_][]const u8{ "priority", "region" };
    _ = try args.selectorFields(allocator, &fields);
    _ = try args.selectorFieldMaxBytes(allocator, 256);

    var table = try args.build(allocator);
    defer table.deinit();
    defer allocator.free(table.get("x-selector-fields").?.array);

    try std.testing.expectEqual(2, table.entries.len);
    const arr = table.get("x-selector-fields").?.array;
    try std.testing.expectEqual(2, arr.len);
    try std.testing.expectEqualSlices(u8, "priority", arr[0].long_string);
    try std.testing.expectEqualSlices(u8, "region", arr[1].long_string);
    try std.testing.expectEqual(256, table.get("x-selector-field-max-bytes").?.i64);
}

test "delayed retry type enum values" {
    try std.testing.expectEqualSlices(u8, "all", DelayedRetryType.all.toSlice());
    try std.testing.expectEqualSlices(u8, "failed", DelayedRetryType.failed.toSlice());
    try std.testing.expectEqualSlices(u8, "returned", DelayedRetryType.returned.toSlice());
}
