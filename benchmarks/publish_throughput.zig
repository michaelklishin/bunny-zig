/// Publish+consume throughput benchmark.
const std = @import("std");
const bunny = @import("bunny");
const Thread = std.Thread;

const Io = std.Io;

fn getIo() Io {
    return Io.Threaded.global_single_threaded.io();
}

const Workload = struct {
    label: []const u8,
    body_size: usize,
    message_count: usize,
};

const workloads = [_]Workload{
    .{ .label = "500K x 64 B", .body_size = 64, .message_count = 500_000 },
    .{ .label = "500K x 512 B", .body_size = 512, .message_count = 500_000 },
    .{ .label = "500K x 1 KB", .body_size = 1024, .message_count = 500_000 },
    .{ .label = "200K x 2 KB", .body_size = 2048, .message_count = 200_000 },
    .{ .label = "200K x 4 KB", .body_size = 4096, .message_count = 200_000 },
    .{ .label = "100K x 8 KB", .body_size = 8192, .message_count = 100_000 },
    .{ .label = "50K x 64 KB", .body_size = 65_536, .message_count = 50_000 },
    .{ .label = "10K x 512 KB", .body_size = 524_288, .message_count = 10_000 },
};

const prefetch: u16 = 500;
const multi_ack_every: usize = 100;
const batch_size: usize = 500;
const queue_name = "bunny-zig.bench";

const RunResult = struct {
    label: []const u8,
    rate: f64,
    mb_sec: f64,
    ms: f64,
};

const Mode = enum { single, batch };

fn nowNs() i96 {
    return Io.Timestamp.now(getIo(), .boot).nanoseconds;
}

fn runWorkload(workload: Workload, mode: Mode) !RunResult {
    const allocator = std.heap.smp_allocator;
    const payload = try allocator.alloc(u8, workload.body_size);
    defer allocator.free(payload);
    @memset(payload, 0xAB);

    const pub_conn = try bunny.Connection.open(allocator, .{
        .recovery = .{ .enabled = false },
    });
    const con_conn = try bunny.Connection.open(allocator, .{
        .recovery = .{ .enabled = false },
    });

    const pub_ch = try pub_conn.openChannel();
    const con_ch = try con_conn.openChannel();

    _ = try con_ch.queueDeclare(queue_name, .{ .exclusive = true });
    try con_ch.basicQos(prefetch, false);
    _ = try con_ch.basicConsume(queue_name, "", .manual);

    // Let the consumer subscription settle
    sleepMs(50);

    const start = nowNs();

    // Publisher thread
    const publisher = try Thread.spawn(.{}, publisherFn, .{ pub_ch, payload, workload.message_count, mode });

    // Consumer in main thread
    var received: usize = 0;
    while (received < workload.message_count) {
        const delivery = try con_ch.recvDelivery() orelse continue;
        received += 1;
        if (@rem(received, multi_ack_every) == 0 or received == workload.message_count) {
            try con_ch.basicAckMultiple(delivery.delivery_tag);
        }
    }

    publisher.join();

    const elapsed_ns = nowNs() - start;
    const ms: f64 = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const rate: f64 = @as(f64, @floatFromInt(workload.message_count)) * 1000.0 / ms;
    const mb_sec: f64 = rate * @as(f64, @floatFromInt(workload.body_size)) / (1024.0 * 1024.0);

    pub_conn.deinit();
    con_conn.deinit();
    // Let the broker clean up the exclusive queue before the next workload
    sleepMs(100);

    return .{ .label = workload.label, .rate = rate, .mb_sec = mb_sec, .ms = ms };
}

fn publisherFn(ch: *bunny.Channel, payload: []const u8, count: usize, mode: Mode) void {
    switch (mode) {
        .single => publishSingle(ch, payload, count),
        .batch => publishBatched(ch, payload, count),
    }
}

fn publishSingle(ch: *bunny.Channel, payload: []const u8, count: usize) void {
    for (0..count) |_| {
        ch.publish(payload, .{
            .routing_key = queue_name,
            .properties = bunny.BasicProperties.transient,
            .flush = .buffered,
        }) catch return;
    }
    ch.flushTransport() catch {};
}

fn publishBatched(ch: *bunny.Channel, payload: []const u8, count: usize) void {
    const allocator = std.heap.smp_allocator;
    const batch_bodies = allocator.alloc([]const u8, batch_size) catch return;
    defer allocator.free(batch_bodies);
    for (batch_bodies) |*b| b.* = payload;

    const full_batches = count / batch_size;
    const remainder = count % batch_size;

    for (0..full_batches) |_| {
        ch.publishBatch(batch_bodies, .{
            .routing_key = queue_name,
            .properties = bunny.BasicProperties.transient,
            .flush = .buffered,
        }) catch return;
    }
    if (remainder > 0) {
        ch.publishBatch(batch_bodies[0..remainder], .{
            .routing_key = queue_name,
            .properties = bunny.BasicProperties.transient,
            .flush = .buffered,
        }) catch return;
    }
    ch.flushTransport() catch {};
}

fn print(comptime fmt: []const u8, args: anytype) void {
    std.debug.print(fmt, args);
}

fn sleepMs(ms: u64) void {
    getIo().sleep(.{ .nanoseconds = ms * std.time.ns_per_ms }, .boot) catch {};
}

pub fn main() !void {
    print("bunny-zig benchmark\n", .{});
    print("Prefetch: {d}, multi-ack every {d}, batch size {d}\n", .{ prefetch, multi_ack_every, batch_size });
    print("{s}\n\n", .{"-" ** 72});

    print("## publish (per-message, buffered flush)\n\n", .{});

    var single_results: [workloads.len]RunResult = undefined;
    for (workloads, 0..) |wl, i| {
        single_results[i] = try runWorkload(wl, .single);
        print("{s:<18}  {d:>8.0} msg/sec  {d:>6.1} MB/sec  ({d:.0} ms)\n", .{
            single_results[i].label, single_results[i].rate, single_results[i].mb_sec, single_results[i].ms,
        });
    }

    print("\n## publishBatch ({d} msgs/batch)\n\n", .{batch_size});

    var batch_results: [workloads.len]RunResult = undefined;
    for (workloads, 0..) |wl, i| {
        batch_results[i] = try runWorkload(wl, .batch);
        print("{s:<18}  {d:>8.0} msg/sec  {d:>6.1} MB/sec  ({d:.0} ms)\n", .{
            batch_results[i].label, batch_results[i].rate, batch_results[i].mb_sec, batch_results[i].ms,
        });
    }

    print("\n{s}\n", .{"-" ** 72});
    print("| Workload           | publish     | batch ({d})  | speedup |\n", .{batch_size});
    print("|:-------------------|------------:|------------:|--------:|\n", .{});
    for (single_results, batch_results) |s, b| {
        const speedup = b.rate / s.rate;
        print("| {s:<18} | {d:>6.0} msg/s | {d:>6.0} msg/s | {d:.2}x |\n", .{
            s.label, s.rate, b.rate, speedup,
        });
    }
    print("{s}\n", .{"-" ** 72});
}
