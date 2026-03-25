/// Automatic connection recovery and topology replay.
const std = @import("std");
const Allocator = std.mem.Allocator;

const log = std.log.scoped(.bunny_recovery);

pub const RecoveryConfig = struct {
    enabled: bool = true,
    initial_interval_ms: u64 = 5_000,
    max_interval_ms: u64 = 60_000,
    backoff_multiplier: f64 = 2.0,
    /// null means unlimited attempts
    max_attempts: ?u32 = null,
};

pub const TopologyEntry = union(enum) {
    exchange: RecordedExchange,
    queue: RecordedQueue,
    queue_binding: RecordedBinding,
    exchange_binding: RecordedBinding,
    consumer: RecordedConsumer,
};

pub const RecordedExchange = struct {
    name: []const u8,
    exchange_type: []const u8,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    channel_id: u16 = 1,
};

pub const RecordedQueue = struct {
    name: []const u8,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    server_named: bool,
    channel_id: u16 = 1,
};

pub const RecordedBinding = struct {
    source: []const u8,
    destination: []const u8,
    routing_key: []const u8,
    channel_id: u16 = 1,
};

pub const RecordedConsumer = struct {
    queue: []const u8,
    consumer_tag: []const u8,
    no_ack: bool,
    exclusive: bool,
    channel_id: u16,
};

pub const RecordedChannel = struct {
    id: u16,
    prefetch_count: u16 = 0,
    prefetch_global: bool = false,
    confirm_mode: bool = false,
};

/// Filter predicate type.
pub const FilterFn = *const fn (TopologyEntry) bool;

/// Controls which entities are replayed during recovery.
pub const TopologyRecoveryFilter = struct {
    exchange_filter: ?*const fn (RecordedExchange) bool = null,
    queue_filter: ?*const fn (RecordedQueue) bool = null,
    queue_binding_filter: ?*const fn (RecordedBinding) bool = null,
    exchange_binding_filter: ?*const fn (RecordedBinding) bool = null,
    consumer_filter: ?*const fn (RecordedConsumer) bool = null,

    pub fn shouldRecover(self: TopologyRecoveryFilter, entry: TopologyEntry) bool {
        return switch (entry) {
            .exchange => |e| if (self.exchange_filter) |f| f(e) else true,
            .queue => |q| if (self.queue_filter) |f| f(q) else true,
            .queue_binding => |b| if (self.queue_binding_filter) |f| f(b) else true,
            .exchange_binding => |b| if (self.exchange_binding_filter) |f| f(b) else true,
            .consumer => |c| if (self.consumer_filter) |f| f(c) else true,
        };
    }
};

/// Tracks declared topology for replay after reconnection.
pub const TopologyRegistry = struct {
    entries: std.ArrayList(TopologyEntry) = .empty,
    channels: std.ArrayList(RecordedChannel) = .empty,
    /// Maps old server-named queue names to new names after recovery.
    queue_name_map: std.StringHashMap([]const u8) = undefined,
    filter: TopologyRecoveryFilter = .{},

    pub fn init(allocator: Allocator) TopologyRegistry {
        return .{
            .queue_name_map = std.StringHashMap([]const u8).init(allocator),
        };
    }

    pub fn recordExchange(self: *TopologyRegistry, allocator: Allocator, ex: RecordedExchange) !void {
        if (ex.name.len == 0) return;
        if (std.mem.startsWith(u8, ex.name, "amq.")) return;
        try self.entries.append(allocator, .{ .exchange = .{
            .name = try allocator.dupe(u8, ex.name),
            .exchange_type = try allocator.dupe(u8, ex.exchange_type),
            .durable = ex.durable,
            .auto_delete = ex.auto_delete,
            .internal = ex.internal,
            .channel_id = ex.channel_id,
        } });
    }

    pub fn recordQueue(self: *TopologyRegistry, allocator: Allocator, q: RecordedQueue) !void {
        try self.entries.append(allocator, .{ .queue = .{
            .name = try allocator.dupe(u8, q.name),
            .durable = q.durable,
            .exclusive = q.exclusive,
            .auto_delete = q.auto_delete,
            .server_named = q.server_named,
            .channel_id = q.channel_id,
        } });
    }

    pub fn recordQueueBinding(self: *TopologyRegistry, allocator: Allocator, b: RecordedBinding) !void {
        try self.entries.append(allocator, .{ .queue_binding = .{
            .source = try allocator.dupe(u8, b.source),
            .destination = try allocator.dupe(u8, b.destination),
            .routing_key = try allocator.dupe(u8, b.routing_key),
            .channel_id = b.channel_id,
        } });
    }

    pub fn recordExchangeBinding(self: *TopologyRegistry, allocator: Allocator, b: RecordedBinding) !void {
        try self.entries.append(allocator, .{ .exchange_binding = .{
            .source = try allocator.dupe(u8, b.source),
            .destination = try allocator.dupe(u8, b.destination),
            .routing_key = try allocator.dupe(u8, b.routing_key),
            .channel_id = b.channel_id,
        } });
    }

    pub fn recordConsumer(self: *TopologyRegistry, allocator: Allocator, c: RecordedConsumer) !void {
        try self.entries.append(allocator, .{ .consumer = .{
            .queue = try allocator.dupe(u8, c.queue),
            .consumer_tag = try allocator.dupe(u8, c.consumer_tag),
            .no_ack = c.no_ack,
            .exclusive = c.exclusive,
            .channel_id = c.channel_id,
        } });
    }

    pub fn recordChannel(self: *TopologyRegistry, allocator: Allocator, ch: RecordedChannel) !void {
        try self.channels.append(allocator, ch);
    }

    /// Resolve a queue name through the rename map (for server-named queues).
    pub fn resolveQueueName(self: *const TopologyRegistry, name: []const u8) []const u8 {
        return self.queue_name_map.get(name) orelse name;
    }

    /// Update queue name in the registry after a server-named queue is redeclared.
    /// The new_name must be a stable pointer (not into the transport read buffer).
    pub fn updateQueueName(self: *TopologyRegistry, allocator: Allocator, old_name: []const u8, new_name: []const u8) void {
        const duped_new = allocator.dupe(u8, new_name) catch return;
        self.queue_name_map.put(old_name, duped_new) catch {};

        for (self.entries.items) |*entry| {
            switch (entry.*) {
                .queue_binding => |*b| {
                    if (std.mem.eql(u8, b.destination, old_name)) {
                        allocator.free(b.destination);
                        b.destination = allocator.dupe(u8, new_name) catch new_name;
                    }
                },
                .consumer => |*c| {
                    if (std.mem.eql(u8, c.queue, old_name)) {
                        allocator.free(c.queue);
                        c.queue = allocator.dupe(u8, new_name) catch new_name;
                    }
                },
                else => {},
            }
        }
    }

    pub fn setFilter(self: *TopologyRegistry, f: TopologyRecoveryFilter) void {
        self.filter = f;
    }

    fn freeEntryStrings(allocator: Allocator, entry: TopologyEntry) void {
        switch (entry) {
            .exchange => |e| {
                allocator.free(e.name);
                allocator.free(e.exchange_type);
            },
            .queue => |q| allocator.free(q.name),
            .queue_binding, .exchange_binding => |b| {
                allocator.free(b.source);
                allocator.free(b.destination);
                allocator.free(b.routing_key);
            },
            .consumer => |c| {
                allocator.free(c.queue);
                allocator.free(c.consumer_tag);
            },
        }
    }

    pub fn clear(self: *TopologyRegistry, allocator: Allocator) void {
        for (self.entries.items) |entry| freeEntryStrings(allocator, entry);
        self.entries.clearAndFree(allocator);
        self.channels.clearAndFree(allocator);
    }

    pub fn deinit(self: *TopologyRegistry, allocator: Allocator) void {
        for (self.entries.items) |entry| freeEntryStrings(allocator, entry);
        self.entries.deinit(allocator);
        self.channels.deinit(allocator);
        var map_it = self.queue_name_map.valueIterator();
        while (map_it.next()) |v| allocator.free(v.*);
        self.queue_name_map.deinit();
    }
};

/// Calculate the next backoff interval in milliseconds.
pub fn nextBackoff(attempt: u32, config: RecoveryConfig) u64 {
    var interval: f64 = @floatFromInt(config.initial_interval_ms);
    for (0..attempt) |_| {
        interval *= config.backoff_multiplier;
    }
    const max: f64 = @floatFromInt(config.max_interval_ms);
    if (interval > max) interval = max;
    return @intFromFloat(interval);
}

// Tests

test "backoff: initial attempt" {
    const config = RecoveryConfig{};
    try std.testing.expectEqual(@as(u64, 5_000), nextBackoff(0, config));
}

test "backoff: exponential growth" {
    const config = RecoveryConfig{};
    try std.testing.expectEqual(@as(u64, 10_000), nextBackoff(1, config));
    try std.testing.expectEqual(@as(u64, 20_000), nextBackoff(2, config));
    try std.testing.expectEqual(@as(u64, 40_000), nextBackoff(3, config));
}

test "backoff: capped at max" {
    const config = RecoveryConfig{};
    try std.testing.expectEqual(@as(u64, 60_000), nextBackoff(10, config));
}

test "topology registry: records and clears" {
    const allocator = std.testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordExchange(allocator, .{
        .name = "my-exchange",
        .exchange_type = "direct",
        .durable = true,
        .auto_delete = false,
        .internal = false,
    });
    try reg.recordQueue(allocator, .{
        .name = "my-queue",
        .durable = true,
        .exclusive = false,
        .auto_delete = false,
        .server_named = false,
    });
    try reg.recordQueueBinding(allocator, .{
        .source = "my-exchange",
        .destination = "my-queue",
        .routing_key = "key",
    });

    try std.testing.expectEqual(@as(usize, 3), reg.entries.items.len);

    reg.clear(allocator);
    try std.testing.expectEqual(@as(usize, 0), reg.entries.items.len);
}

test "topology registry: skips predeclared exchanges" {
    const allocator = std.testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordExchange(allocator, .{
        .name = "amq.direct",
        .exchange_type = "direct",
        .durable = true,
        .auto_delete = false,
        .internal = false,
    });
    try reg.recordExchange(allocator, .{
        .name = "",
        .exchange_type = "direct",
        .durable = true,
        .auto_delete = false,
        .internal = false,
    });

    try std.testing.expectEqual(@as(usize, 0), reg.entries.items.len);
}

test "topology recovery filter: accepts all by default" {
    const filter = TopologyRecoveryFilter{};
    try std.testing.expect(filter.shouldRecover(.{ .exchange = .{
        .name = "test",
        .exchange_type = "direct",
        .durable = true,
        .auto_delete = false,
        .internal = false,
    } }));
}

test "topology recovery filter: rejects filtered entries" {
    const filter = TopologyRecoveryFilter{
        .exchange_filter = &struct {
            fn f(ex: RecordedExchange) bool {
                return !std.mem.eql(u8, ex.name, "skip-me");
            }
        }.f,
    };

    try std.testing.expect(!filter.shouldRecover(.{ .exchange = .{
        .name = "skip-me",
        .exchange_type = "direct",
        .durable = true,
        .auto_delete = false,
        .internal = false,
    } }));
    try std.testing.expect(filter.shouldRecover(.{ .exchange = .{
        .name = "keep-me",
        .exchange_type = "direct",
        .durable = true,
        .auto_delete = false,
        .internal = false,
    } }));
}

test "topology registry: queue name mapping" {
    const allocator = std.testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordQueue(allocator, .{
        .name = "amq.gen-old",
        .durable = false,
        .exclusive = false,
        .auto_delete = true,
        .server_named = true,
    });
    try reg.recordQueueBinding(allocator, .{
        .source = "my-exchange",
        .destination = "amq.gen-old",
        .routing_key = "",
    });
    try reg.recordConsumer(allocator, .{
        .queue = "amq.gen-old",
        .consumer_tag = "ctag-1",
        .no_ack = false,
        .exclusive = false,
        .channel_id = 1,
    });

    reg.updateQueueName(allocator, "amq.gen-old", "amq.gen-new");

    try std.testing.expectEqualSlices(u8, "amq.gen-new", reg.resolveQueueName("amq.gen-old"));

    // Bindings and consumers should be updated
    const binding = reg.entries.items[1].queue_binding;
    try std.testing.expectEqualSlices(u8, "amq.gen-new", binding.destination);

    const consumer = reg.entries.items[2].consumer;
    try std.testing.expectEqualSlices(u8, "amq.gen-new", consumer.queue);
}

test "topology registry: channel recording" {
    const allocator = std.testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordChannel(allocator, .{ .id = 1, .prefetch_count = 10 });
    try reg.recordChannel(allocator, .{ .id = 2, .confirm_mode = true });

    try std.testing.expectEqual(@as(usize, 2), reg.channels.items.len);
    try std.testing.expectEqual(@as(u16, 10), reg.channels.items[0].prefetch_count);
    try std.testing.expect(reg.channels.items[1].confirm_mode);
}
