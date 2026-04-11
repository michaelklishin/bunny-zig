/// Thread pool for dispatching consumer deliveries off the reader thread.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Thread = std.Thread;
const Io = std.Io;
const Mutex = Io.Mutex;
const Condition = Io.Condition;

const channel_mod = @import("channel.zig");
const Delivery = channel_mod.Delivery;

/// Get a usable Io context for mutex/condition operations.
fn getIo() Io {
    return std.Io.Threaded.global_single_threaded.io();
}

const WorkItem = struct {
    handler: *const fn (Delivery) void,
    delivery: Delivery,
};

pub const ConsumerWorkPool = struct {
    workers: []Thread,
    queue: std.ArrayList(WorkItem),
    mutex: Mutex = .init,
    signal: Condition = .init,
    should_stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    allocator: Allocator,

    /// Create a heap-allocated pool so worker thread pointers remain valid.
    pub fn init(allocator: Allocator, pool_size: u16) !*ConsumerWorkPool {
        const pool = try allocator.create(ConsumerWorkPool);
        const pool_io = getIo();
        pool.* = .{
            .workers = &.{},
            .queue = .empty,
            .allocator = allocator,
        };

        const size: usize = @max(1, pool_size);
        const workers = try allocator.alloc(Thread, size);
        var started: usize = 0;
        errdefer {
            pool.should_stop.store(true, .release);
            pool.signal.broadcast(pool_io);
            for (workers[0..started]) |w| w.join();
            allocator.free(workers);
            pool.queue.deinit(allocator);
            allocator.destroy(pool);
        }

        for (workers) |*w| {
            w.* = try Thread.spawn(.{}, workerLoop, .{pool});
            started += 1;
        }
        pool.workers = workers;

        return pool;
    }

    pub fn submit(self: *ConsumerWorkPool, handler: *const fn (Delivery) void, delivery: Delivery) void {
        const io = getIo();
        self.mutex.lockUncancelable(io);
        self.queue.append(self.allocator, .{ .handler = handler, .delivery = delivery }) catch {
            self.mutex.unlock(io);
            return;
        };
        self.signal.signal(io);
        self.mutex.unlock(io);
    }

    pub fn shutdown(self: *ConsumerWorkPool) void {
        const io = getIo();
        self.should_stop.store(true, .release);

        self.mutex.lockUncancelable(io);
        self.signal.broadcast(io);
        self.mutex.unlock(io);

        for (self.workers) |w| w.join();
        const allocator = self.allocator;
        self.queue.deinit(allocator);
        allocator.free(self.workers);
        allocator.destroy(self);
    }

    fn workerLoop(pool: *ConsumerWorkPool) void {
        const io = getIo();
        while (true) {
            pool.mutex.lockUncancelable(io);
            while (pool.queue.items.len == 0 and !pool.should_stop.load(.acquire)) {
                pool.signal.waitUncancelable(io, &pool.mutex);
            }
            if (pool.queue.items.len == 0) {
                pool.mutex.unlock(io);
                return;
            }
            const item = pool.queue.orderedRemove(0);
            pool.mutex.unlock(io);

            item.handler(item.delivery);
        }
    }
};

test "work pool: dispatches items" {
    const pool = try ConsumerWorkPool.init(std.testing.allocator, 2);
    defer pool.shutdown();

    var counter = std.atomic.Value(u32).init(0);

    const Handler = struct {
        var cnt: *std.atomic.Value(u32) = undefined;
        fn handle(_: Delivery) void {
            _ = cnt.fetchAdd(1, .monotonic);
        }
    };
    Handler.cnt = &counter;

    const dummy = Delivery{
        .consumer_tag = "",
        .delivery_tag = 0,
        .redelivered = false,
        .exchange = "",
        .routing_key = "",
        .properties = @import("protocol.zig").properties.BasicProperties.default,
        .body = "",
    };

    for (0..10) |_| {
        pool.submit(&Handler.handle, dummy);
    }

    // Spin-wait for all items to be processed
    var attempts: u32 = 0;
    while (counter.load(.acquire) < 10 and attempts < 1000) : (attempts += 1) {
        // Busy-wait with a short yield
        std.atomic.spinLoopHint();
    }
    // If spin-wait wasn't enough, use futex-based sleep
    if (counter.load(.acquire) < 10) {
        const io = getIo();
        for (0..100) |_| {
            if (counter.load(.acquire) >= 10) break;
            io.sleep(.{ .nanoseconds = 10 * std.time.ns_per_ms }, .boot) catch {};
        }
    }
    try std.testing.expectEqual(10, counter.load(.acquire));
}
