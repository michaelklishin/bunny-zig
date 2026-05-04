const std = @import("std");
const bunny = @import("bunny");
const testing = std.testing;

const TopologyRegistry = bunny.TopologyRegistry;

// Verifies the schema additions for per-consumer QoS replay: `RecordedConsumer`
// gained `prefetch_count` and `prefetch_global` so each consumer keeps its own
// QoS scope across a reconnect.

test "recordConsumer: per-consumer prefetch is preserved" {
    const allocator = testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordConsumer(allocator, .{
        .queue = "q1",
        .consumer_tag = "tag-1",
        .no_ack = false,
        .exclusive = false,
        .channel_id = 1,
        .prefetch_count = 5,
        .prefetch_global = false,
    });
    try reg.recordConsumer(allocator, .{
        .queue = "q2",
        .consumer_tag = "tag-2",
        .no_ack = false,
        .exclusive = false,
        .channel_id = 1,
        .prefetch_count = 50,
        .prefetch_global = true,
    });

    const c1 = reg.entries.items[0].consumer;
    try testing.expectEqual(@as(u16, 5), c1.prefetch_count);
    try testing.expect(!c1.prefetch_global);

    const c2 = reg.entries.items[1].consumer;
    try testing.expectEqual(@as(u16, 50), c2.prefetch_count);
    try testing.expect(c2.prefetch_global);
}

test "recordConsumer: default prefetch is zero (no per-consumer replay)" {
    const allocator = testing.allocator;
    var reg = TopologyRegistry.init(allocator);
    defer reg.deinit(allocator);

    try reg.recordConsumer(allocator, .{
        .queue = "q",
        .consumer_tag = "tag",
        .no_ack = false,
        .exclusive = false,
        .channel_id = 1,
    });

    const c = reg.entries.items[0].consumer;
    try testing.expectEqual(@as(u16, 0), c.prefetch_count);
    try testing.expect(!c.prefetch_global);
}

test "recordConsumer: a wide range of prefetch values roundtrips faithfully" {
    const allocator = testing.allocator;
    const samples = [_]u16{ 0, 1, 2, 10, 100, 1024, 32_767, 32_768, 65_535 };
    for (samples) |n| {
        var reg = TopologyRegistry.init(allocator);
        defer reg.deinit(allocator);
        try reg.recordConsumer(allocator, .{
            .queue = "q",
            .consumer_tag = "tag",
            .no_ack = false,
            .exclusive = false,
            .channel_id = 1,
            .prefetch_count = n,
            .prefetch_global = (n & 1) == 1,
        });
        try testing.expectEqual(n, reg.entries.items[0].consumer.prefetch_count);
        try testing.expectEqual((n & 1) == 1, reg.entries.items[0].consumer.prefetch_global);
    }
}
