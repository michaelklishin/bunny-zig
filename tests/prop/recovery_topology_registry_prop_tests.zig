//! Property-based tests for `src/recovery.zig`'s `TopologyRegistry`.
const std = @import("std");
const pt = @import("proptest");
const bunny = @import("bunny");

const TopologyRegistry = bunny.TopologyRegistry;

const allocator = std.testing.allocator;

const exchange_names = [_][]const u8{
    "ex0", "ex1", "ex2", "ex3", "ex4", "ex5", "ex6", "ex7",
};
const queue_names = [_][]const u8{
    "q0", "q1", "q2", "q3", "q4", "q5", "q6", "q7",
};
const exchange_types = [_][]const u8{ "direct", "fanout", "topic", "headers" };
const routing_keys = [_][]const u8{ "rk.a", "rk.b", "rk.c", "rk.d" };

fn nameIndex(names: []const []const u8, target: []const u8) ?usize {
    for (names, 0..) |n, i| {
        if (std.mem.eql(u8, n, target)) return i;
    }
    return null;
}

fn exchangeDedup(seeds: []const u8) !void {
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    for (seeds) |s| {
        try reg.recordExchange(allocator, .{
            .name = exchange_names[s & 0x7],
            .exchange_type = exchange_types[(s >> 3) & 0x3],
            .durable = (s >> 5) & 1 != 0,
            .auto_delete = false,
            .internal = false,
        });
    }

    var seen = std.bit_set.IntegerBitSet(8).initEmpty();
    for (seeds) |s| seen.set(s & 0x7);

    if (reg.entries.items.len != seen.count()) return error.WrongEntryCount;

    var last_type: [8]usize = .{0} ** 8;
    var last_durable: [8]bool = .{false} ** 8;
    var present: [8]bool = .{false} ** 8;
    for (seeds) |s| {
        const ni = s & 0x7;
        present[ni] = true;
        last_type[ni] = (s >> 3) & 0x3;
        last_durable[ni] = (s >> 5) & 1 != 0;
    }

    for (reg.entries.items) |entry| {
        const e = entry.exchange;
        const i = nameIndex(&exchange_names, e.name) orelse return error.UnknownName;
        if (!present[i]) return error.UnexpectedEntry;
        if (!std.mem.eql(u8, e.exchange_type, exchange_types[last_type[i]])) return error.TypeMismatch;
        if (e.durable != last_durable[i]) return error.DurableMismatch;
    }
}

fn predeclaredAlwaysSkipped(seeds: []const u8) !void {
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    const skip_names = [_][]const u8{ "", "amq.direct", "amq.topic", "amq.fanout", "amq.foo" };

    for (seeds) |s| {
        const name = if (s & 1 != 0)
            skip_names[(s >> 1) % skip_names.len]
        else
            exchange_names[(s >> 1) & 0x7];
        try reg.recordExchange(allocator, .{
            .name = name,
            .exchange_type = "direct",
            .durable = true,
            .auto_delete = false,
            .internal = false,
        });
    }

    for (reg.entries.items) |entry| {
        const name = entry.exchange.name;
        if (name.len == 0) return error.EmptyNameLeaked;
        if (std.mem.startsWith(u8, name, "amq.")) return error.AmqPrefixLeaked;
    }
}

fn queueDedup(seeds: []const u8) !void {
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    for (seeds) |s| {
        try reg.recordQueue(allocator, .{
            .name = queue_names[s & 0x7],
            .durable = (s >> 3) & 1 != 0,
            .exclusive = (s >> 4) & 1 != 0,
            .auto_delete = (s >> 5) & 1 != 0,
            .server_named = false,
        });
    }

    var seen = std.bit_set.IntegerBitSet(8).initEmpty();
    for (seeds) |s| seen.set(s & 0x7);

    if (reg.entries.items.len != seen.count()) return error.WrongEntryCount;

    var last_durable: [8]bool = .{false} ** 8;
    var last_exclusive: [8]bool = .{false} ** 8;
    var last_auto_delete: [8]bool = .{false} ** 8;
    for (seeds) |s| {
        const ni = s & 0x7;
        last_durable[ni] = (s >> 3) & 1 != 0;
        last_exclusive[ni] = (s >> 4) & 1 != 0;
        last_auto_delete[ni] = (s >> 5) & 1 != 0;
    }

    for (reg.entries.items) |entry| {
        const q = entry.queue;
        const i = nameIndex(&queue_names, q.name) orelse return error.UnknownName;
        if (q.durable != last_durable[i]) return error.DurableMismatch;
        if (q.exclusive != last_exclusive[i]) return error.ExclusiveMismatch;
        if (q.auto_delete != last_auto_delete[i]) return error.AutoDeleteMismatch;
    }
}

// Each seed packs ex_idx (3 bits), q_idx (3 bits), rk_idx (2 bits) into
// the low byte; that byte uniquely determines the binding triple.
fn queueBindingDedup(seeds: []const u16) !void {
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    var unique = std.AutoHashMap(u8, void).init(allocator);
    defer unique.deinit();

    for (seeds) |raw| {
        const s: u8 = @truncate(raw);
        try reg.recordQueueBinding(allocator, .{
            .source = exchange_names[s & 0x7],
            .destination = queue_names[(s >> 3) & 0x7],
            .routing_key = routing_keys[(s >> 6) & 0x3],
        });
        try unique.put(s, {});
    }

    if (reg.entries.items.len != unique.count()) return error.WrongEntryCount;
}

fn exchangeBindingDedup(seeds: []const u16) !void {
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    var unique = std.AutoHashMap(u8, void).init(allocator);
    defer unique.deinit();

    for (seeds) |raw| {
        const s: u8 = @truncate(raw);
        try reg.recordExchangeBinding(allocator, .{
            .source = exchange_names[s & 0x7],
            .destination = exchange_names[(s >> 3) & 0x7],
            .routing_key = routing_keys[(s >> 6) & 0x3],
        });
        try unique.put(s, {});
    }

    if (reg.entries.items.len != unique.count()) return error.WrongEntryCount;
}

// recordConsumer dedups by consumer_tag. The first record wins (unlike
// exchange/queue which last-write-win), so once a tag is seen, later
// duplicates with different fields must not be applied.
fn consumerTagDedup(seeds: []const u16) !void {
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    var unique = std.AutoHashMap(u8, void).init(allocator);
    defer unique.deinit();
    var first_no_ack: [8]bool = .{false} ** 8;
    var first_seen: [8]bool = .{false} ** 8;

    for (seeds) |raw| {
        const tag_idx: u8 = @as(u8, @truncate(raw)) & 0x7;
        const queue_idx: u8 = @as(u8, @truncate(raw >> 3)) & 0x7;
        const no_ack = (raw >> 6) & 1 != 0;

        var tag_buf: [16]u8 = undefined;
        const tag = std.fmt.bufPrint(&tag_buf, "ctag-{d}", .{tag_idx}) catch unreachable;

        try reg.recordConsumer(allocator, .{
            .queue = queue_names[queue_idx],
            .consumer_tag = tag,
            .no_ack = no_ack,
            .exclusive = false,
            .channel_id = 1,
        });

        if (!first_seen[tag_idx]) {
            first_seen[tag_idx] = true;
            first_no_ack[tag_idx] = no_ack;
        }
        try unique.put(tag_idx, {});
    }

    if (reg.entries.items.len != unique.count()) return error.WrongEntryCount;

    for (reg.entries.items) |entry| {
        const c = entry.consumer;
        const idx_str = c.consumer_tag[5..]; // skip the "ctag-" prefix
        const idx = std.fmt.parseInt(u8, idx_str, 10) catch return error.UnparseableTag;
        if (idx >= 8) return error.IndexOutOfRange;
        if (c.no_ack != first_no_ack[idx]) return error.FirstWriteNotPreserved;
    }
}

fn channelDedup(seeds: []const u32) !void {
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    for (seeds) |s| {
        try reg.recordChannel(allocator, .{
            .id = @as(u16, @truncate(s & 0xF)) + 1, // ids 1..16
            .prefetch_count = @truncate(s >> 8),
            .confirm_mode = (s >> 24) & 1 != 0,
        });
    }

    var seen = std.bit_set.IntegerBitSet(16).initEmpty();
    for (seeds) |s| seen.set(s & 0xF);
    if (reg.channels.items.len != seen.count()) return error.WrongChannelCount;

    var last_prefetch: [17]u16 = .{0} ** 17;
    var last_confirm: [17]bool = .{false} ** 17;
    for (seeds) |s| {
        const id: u16 = @as(u16, @truncate(s & 0xF)) + 1;
        last_prefetch[id] = @truncate(s >> 8);
        last_confirm[id] = (s >> 24) & 1 != 0;
    }
    for (reg.channels.items) |ch| {
        if (ch.prefetch_count != last_prefetch[ch.id]) return error.PrefetchMismatch;
        if (ch.confirm_mode != last_confirm[ch.id]) return error.ConfirmMismatch;
    }
}

fn renamePropagates(p: struct { u8, u8, []const u8 }) !void {
    const ren_q_idx = p[0] & 0x7;
    var other_q_idx = p[1] & 0x7;
    if (ren_q_idx == other_q_idx) other_q_idx = (other_q_idx +% 1) & 0x7;
    const seeds = p[2];

    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    const renamed = queue_names[ren_q_idx];
    const other = queue_names[other_q_idx];
    const new_name = "renamed-target";

    try reg.recordQueue(allocator, .{
        .name = renamed,
        .durable = false,
        .exclusive = false,
        .auto_delete = true,
        .server_named = true,
    });
    try reg.recordQueue(allocator, .{
        .name = other,
        .durable = false,
        .exclusive = false,
        .auto_delete = true,
        .server_named = false,
    });

    for (seeds) |s| {
        const target = if ((s >> 3) & 1 != 0) renamed else other;
        try reg.recordQueueBinding(allocator, .{
            .source = exchange_names[s & 0x7],
            .destination = target,
            .routing_key = routing_keys[(s >> 4) & 0x3],
        });
    }
    for (seeds, 0..) |s, i| {
        const target = if ((s >> 6) & 1 != 0) renamed else other;
        var tag_buf: [16]u8 = undefined;
        const tag = std.fmt.bufPrint(&tag_buf, "ctag-{d}", .{i}) catch unreachable;
        try reg.recordConsumer(allocator, .{
            .queue = target,
            .consumer_tag = tag,
            .no_ack = false,
            .exclusive = false,
            .channel_id = 1,
        });
    }

    try reg.updateQueueName(allocator, renamed, new_name);

    if (!std.mem.eql(u8, reg.resolveQueueName(renamed), new_name)) return error.ResolveBroken;
    if (!std.mem.eql(u8, reg.resolveQueueName(other), other)) return error.OtherQueueMisresolved;

    for (reg.entries.items) |entry| {
        switch (entry) {
            .queue_binding => |b| {
                if (std.mem.eql(u8, b.destination, renamed)) return error.BindingNotRewritten;
            },
            .consumer => |c| {
                if (std.mem.eql(u8, c.queue, renamed)) return error.ConsumerNotRewritten;
            },
            else => {},
        }
    }
}

test "registry prop: recordExchange dedups by name with last-write-wins" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.bytes(0, 32), exchangeDedup);
}

test "registry prop: predeclared exchanges are never recorded" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.bytes(0, 32), predeclaredAlwaysSkipped);
}

test "registry prop: recordQueue dedups by name with last-write-wins" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.bytes(0, 32), queueDedup);
}

test "registry prop: queue bindings dedup by (source, destination, routing_key)" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.slice(pt.num.int(u16), 0, 32), queueBindingDedup);
}

test "registry prop: exchange bindings dedup by (source, destination, routing_key)" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.slice(pt.num.int(u16), 0, 32), exchangeBindingDedup);
}

test "registry prop: recordConsumer dedups by tag with first-write-wins" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.slice(pt.num.int(u16), 0, 32), consumerTagDedup);
}

test "registry prop: recordChannel dedups by id with last-write-wins" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    try runner.check(allocator, pt.collection.slice(pt.num.int(u32), 0, 32), channelDedup);
}

test "registry prop: updateQueueName rewrites every reference to the renamed queue" {
    var runner = pt.Runner.initDefault();
    defer runner.deinit();
    const strat = pt.tuple.t3(
        pt.num.int(u8),
        pt.num.int(u8),
        pt.collection.bytes(0, 16),
    );
    try runner.check(allocator, strat, renamePropagates);
}
