/// Automatic connection recovery and topology replay.
const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("protocol.zig").types;
const FieldTable = types.FieldTable;

const log = std.log.scoped(.bunny_recovery);

pub const RecoveryConfig = struct {
    enabled: bool = true,
    initial_interval_ms: u64 = 5_000,
    max_interval_ms: u64 = 60_000,
    backoff_multiplier: f64 = 2.0,
    /// null means unlimited attempts
    max_attempts: ?u32 = null,
    /// Random jitter as a fraction of the base backoff, applied per attempt to
    /// avoid thundering herds during a coordinated reconnect (e.g. broker
    /// restart). 0.0 disables jitter; 0.2 means up to 20% additional delay.
    jitter_fraction: f64 = 0.0,
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
    arguments: FieldTable = FieldTable.empty,
};

pub const RecordedQueue = struct {
    name: []const u8,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    server_named: bool,
    channel_id: u16 = 1,
    arguments: FieldTable = FieldTable.empty,
};

pub const RecordedBinding = struct {
    source: []const u8,
    destination: []const u8,
    routing_key: []const u8,
    channel_id: u16 = 1,
    arguments: FieldTable = FieldTable.empty,
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

        // Update in place if already recorded
        for (self.entries.items) |*entry| {
            switch (entry.*) {
                .exchange => |*e| if (std.mem.eql(u8, e.name, ex.name)) {
                    allocator.free(e.exchange_type);
                    e.exchange_type = try allocator.dupe(u8, ex.exchange_type);
                    e.durable = ex.durable;
                    e.auto_delete = ex.auto_delete;
                    e.internal = ex.internal;
                    e.channel_id = ex.channel_id;
                    e.arguments = ex.arguments;
                    return;
                },
                else => {},
            }
        }

        try self.entries.append(allocator, .{ .exchange = .{
            .name = try allocator.dupe(u8, ex.name),
            .exchange_type = try allocator.dupe(u8, ex.exchange_type),
            .durable = ex.durable,
            .auto_delete = ex.auto_delete,
            .internal = ex.internal,
            .channel_id = ex.channel_id,
            .arguments = ex.arguments,
        } });
    }

    pub fn recordQueue(self: *TopologyRegistry, allocator: Allocator, q: RecordedQueue) !void {
        // Update in place if already recorded
        for (self.entries.items) |*entry| {
            switch (entry.*) {
                .queue => |*existing| if (std.mem.eql(u8, existing.name, q.name)) {
                    existing.durable = q.durable;
                    existing.exclusive = q.exclusive;
                    existing.auto_delete = q.auto_delete;
                    existing.server_named = q.server_named;
                    existing.channel_id = q.channel_id;
                    existing.arguments = q.arguments;
                    return;
                },
                else => {},
            }
        }

        try self.entries.append(allocator, .{ .queue = .{
            .name = try allocator.dupe(u8, q.name),
            .durable = q.durable,
            .exclusive = q.exclusive,
            .auto_delete = q.auto_delete,
            .server_named = q.server_named,
            .channel_id = q.channel_id,
            .arguments = q.arguments,
        } });
    }

    pub fn recordQueueBinding(self: *TopologyRegistry, allocator: Allocator, b: RecordedBinding) !void {
        // Skip if an identical binding already exists
        for (self.entries.items) |entry| {
            switch (entry) {
                .queue_binding => |existing| if (std.mem.eql(u8, existing.source, b.source) and
                    std.mem.eql(u8, existing.destination, b.destination) and
                    std.mem.eql(u8, existing.routing_key, b.routing_key)) return,
                else => {},
            }
        }

        try self.entries.append(allocator, .{ .queue_binding = .{
            .source = try allocator.dupe(u8, b.source),
            .destination = try allocator.dupe(u8, b.destination),
            .routing_key = try allocator.dupe(u8, b.routing_key),
            .channel_id = b.channel_id,
            .arguments = b.arguments,
        } });
    }

    pub fn recordExchangeBinding(self: *TopologyRegistry, allocator: Allocator, b: RecordedBinding) !void {
        // Skip if an identical binding already exists
        for (self.entries.items) |entry| {
            switch (entry) {
                .exchange_binding => |existing| if (std.mem.eql(u8, existing.source, b.source) and
                    std.mem.eql(u8, existing.destination, b.destination) and
                    std.mem.eql(u8, existing.routing_key, b.routing_key)) return,
                else => {},
            }
        }

        try self.entries.append(allocator, .{ .exchange_binding = .{
            .source = try allocator.dupe(u8, b.source),
            .destination = try allocator.dupe(u8, b.destination),
            .routing_key = try allocator.dupe(u8, b.routing_key),
            .channel_id = b.channel_id,
            .arguments = b.arguments,
        } });
    }

    pub fn recordConsumer(self: *TopologyRegistry, allocator: Allocator, c: RecordedConsumer) !void {
        // Skip if this consumer tag is already recorded
        for (self.entries.items) |entry| {
            switch (entry) {
                .consumer => |existing| if (std.mem.eql(u8, existing.consumer_tag, c.consumer_tag)) return,
                else => {},
            }
        }

        try self.entries.append(allocator, .{ .consumer = .{
            .queue = try allocator.dupe(u8, c.queue),
            .consumer_tag = try allocator.dupe(u8, c.consumer_tag),
            .no_ack = c.no_ack,
            .exclusive = c.exclusive,
            .channel_id = c.channel_id,
        } });
    }

    pub fn recordChannel(self: *TopologyRegistry, allocator: Allocator, ch: RecordedChannel) !void {
        // Update in place if already recorded
        for (self.channels.items) |*existing| {
            if (existing.id == ch.id) {
                existing.* = ch;
                return;
            }
        }
        try self.channels.append(allocator, ch);
    }

    /// Resolve a queue name through the rename map (for server-named queues).
    pub fn resolveQueueName(self: *const TopologyRegistry, name: []const u8) []const u8 {
        return self.queue_name_map.get(name) orelse name;
    }

    /// Update queue name in the registry after a server-named queue is redeclared.
    pub fn updateQueueName(self: *TopologyRegistry, allocator: Allocator, old_name: []const u8, new_name: []const u8) !void {
        // The map owns both keys and values: the caller's old_name lives
        // inside a topology entry that may be reassigned later in the same
        // recovery pass, and ok.queue aliases a transient frame buffer.
        // getOrPut lets us reuse the existing key allocation when a previous
        // recovery already inserted this old_name; we only dupe on insert.
        const new_value = try allocator.dupe(u8, new_name);
        errdefer allocator.free(new_value);

        const gop = try self.queue_name_map.getOrPut(old_name);
        if (gop.found_existing) {
            allocator.free(gop.value_ptr.*);
            gop.value_ptr.* = new_value;
        } else {
            const duped_key = allocator.dupe(u8, old_name) catch |err| {
                self.queue_name_map.removeByPtr(gop.key_ptr);
                return err;
            };
            gop.key_ptr.* = duped_key;
            gop.value_ptr.* = new_value;
        }

        for (self.entries.items) |*entry| {
            switch (entry.*) {
                .queue_binding => |*b| {
                    if (std.mem.eql(u8, b.destination, old_name)) {
                        allocator.free(b.destination);
                        b.destination = try allocator.dupe(u8, new_name);
                    }
                },
                .consumer => |*c| {
                    if (std.mem.eql(u8, c.queue, old_name)) {
                        allocator.free(c.queue);
                        c.queue = try allocator.dupe(u8, new_name);
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
        var map_it = self.queue_name_map.iterator();
        while (map_it.next()) |kv| {
            allocator.free(kv.key_ptr.*);
            allocator.free(kv.value_ptr.*);
        }
        self.queue_name_map.deinit();
    }
};

/// Calculate the next backoff interval in milliseconds (deterministic base).
pub fn nextBackoff(attempt: u32, config: RecoveryConfig) u64 {
    var interval: f64 = @floatFromInt(config.initial_interval_ms);
    for (0..attempt) |_| {
        interval *= config.backoff_multiplier;
    }
    const max: f64 = @floatFromInt(config.max_interval_ms);
    if (interval > max) interval = max;
    return @intFromFloat(interval);
}

/// Apply additive jitter on top of a base backoff. The result is in
/// [base_ms, base_ms * (1 + jitter_fraction)].
pub fn applyJitter(base_ms: u64, jitter_fraction: f64, random: std.Random) u64 {
    if (jitter_fraction <= 0.0 or base_ms == 0) return base_ms;
    const base: f64 = @floatFromInt(base_ms);
    const extra = base * jitter_fraction * random.float(f64);
    return base_ms + @as(u64, @intFromFloat(extra));
}

// Tests

test "backoff: initial attempt" {
    const config = RecoveryConfig{};
    try std.testing.expectEqual(5_000, nextBackoff(0, config));
}

test "backoff: exponential growth" {
    const config = RecoveryConfig{};
    try std.testing.expectEqual(10_000, nextBackoff(1, config));
    try std.testing.expectEqual(20_000, nextBackoff(2, config));
    try std.testing.expectEqual(40_000, nextBackoff(3, config));
}

test "backoff: capped at max" {
    const config = RecoveryConfig{};
    try std.testing.expectEqual(60_000, nextBackoff(10, config));
}

test "backoff: jitter is zero by default" {
    var prng = std.Random.DefaultPrng.init(0);
    try std.testing.expectEqual(5_000, applyJitter(5_000, 0.0, prng.random()));
}

test "backoff: jitter stays within [base, base * (1 + fraction)]" {
    var prng = std.Random.DefaultPrng.init(42);
    const base: u64 = 1_000;
    const fraction: f64 = 0.2;
    for (0..200) |_| {
        const jittered = applyJitter(base, fraction, prng.random());
        try std.testing.expect(jittered >= base);
        try std.testing.expect(jittered <= 1_200);
    }
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

    try std.testing.expectEqual(3, reg.entries.items.len);

    reg.clear(allocator);
    try std.testing.expectEqual(0, reg.entries.items.len);
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

    try std.testing.expectEqual(0, reg.entries.items.len);
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

    try reg.updateQueueName(allocator, "amq.gen-old", "amq.gen-new");

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

    try std.testing.expectEqual(2, reg.channels.items.len);
    try std.testing.expectEqual(10, reg.channels.items[0].prefetch_count);
    try std.testing.expect(reg.channels.items[1].confirm_mode);
}

test "topology registry: exchange re-declaration updates in place" {
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
    try reg.recordExchange(allocator, .{
        .name = "my-exchange",
        .exchange_type = "fanout",
        .durable = false,
        .auto_delete = true,
        .internal = false,
    });

    try std.testing.expectEqual(1, reg.entries.items.len);
    const ex = reg.entries.items[0].exchange;
    try std.testing.expectEqualSlices(u8, "fanout", ex.exchange_type);
    try std.testing.expect(!ex.durable);
    try std.testing.expect(ex.auto_delete);
}

test "topology registry: queue re-declaration updates in place" {
    const allocator = std.testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordQueue(allocator, .{
        .name = "my-queue",
        .durable = true,
        .exclusive = false,
        .auto_delete = false,
        .server_named = false,
    });
    try reg.recordQueue(allocator, .{
        .name = "my-queue",
        .durable = false,
        .exclusive = true,
        .auto_delete = true,
        .server_named = false,
    });

    try std.testing.expectEqual(1, reg.entries.items.len);
    const q = reg.entries.items[0].queue;
    try std.testing.expect(!q.durable);
    try std.testing.expect(q.exclusive);
}

test "topology registry: duplicate bindings are skipped" {
    const allocator = std.testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordQueueBinding(allocator, .{
        .source = "ex",
        .destination = "q",
        .routing_key = "rk",
    });
    try reg.recordQueueBinding(allocator, .{
        .source = "ex",
        .destination = "q",
        .routing_key = "rk",
    });
    // Different routing key is a distinct binding
    try reg.recordQueueBinding(allocator, .{
        .source = "ex",
        .destination = "q",
        .routing_key = "other",
    });

    try std.testing.expectEqual(2, reg.entries.items.len);
}

test "topology registry: duplicate consumers are skipped" {
    const allocator = std.testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordConsumer(allocator, .{
        .queue = "q",
        .consumer_tag = "ctag-1",
        .no_ack = false,
        .exclusive = false,
        .channel_id = 1,
    });
    try reg.recordConsumer(allocator, .{
        .queue = "q",
        .consumer_tag = "ctag-1",
        .no_ack = true,
        .exclusive = false,
        .channel_id = 1,
    });

    try std.testing.expectEqual(1, reg.entries.items.len);
}

test "topology registry: duplicate channel deduplicates by id" {
    const allocator = std.testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordChannel(allocator, .{ .id = 1, .prefetch_count = 10 });
    try reg.recordChannel(allocator, .{ .id = 1, .prefetch_count = 50, .confirm_mode = true });

    try std.testing.expectEqual(1, reg.channels.items.len);
    try std.testing.expectEqual(50, reg.channels.items[0].prefetch_count);
    try std.testing.expect(reg.channels.items[0].confirm_mode);
}
