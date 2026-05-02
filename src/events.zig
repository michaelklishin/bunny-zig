/// Connection and channel event types.

pub const ConnectionEvent = union(enum) {
    blocked: []const u8,
    unblocked: void,
    closed: struct { code: u16, text: []const u8 },
    recovery_started: void,
    recovery_succeeded: void,
    recovery_failed: []const u8,
    recovery_queue_name_changed: struct { old_name: []const u8, new_name: []const u8 },
    recovery_consumer_tag_changed: struct { old_tag: []const u8, new_tag: []const u8 },
};

pub const ChannelEvent = union(enum) {
    closed: struct {
        code: u16,
        text: []const u8,
        initiated_by_server: bool,
    },
    recovered: void,
    flow: bool,
    consumer_cancelled: []const u8,
    message_returned: void,
};

/// A thread-safe list of event listeners.
/// Listeners run synchronously on the reader thread, so they must be infallible
/// (no panics) and return quickly. Defer heavy work to the application's own
/// thread pool.
pub fn EventListeners(comptime EventType: type) type {
    const std = @import("std");
    const Allocator = std.mem.Allocator;
    const Io = std.Io;
    const Mutex = Io.Mutex;

    return struct {
        const Self = @This();
        const Callback = *const fn (EventType) void;

        callbacks: std.ArrayList(Callback) = .empty,
        mutex: Mutex = .init,

        fn getIo() Io {
            return Io.Threaded.global_single_threaded.io();
        }

        pub fn add(self: *Self, allocator: Allocator, cb: Callback) !void {
            const io = getIo();
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            try self.callbacks.append(allocator, cb);
        }

        pub fn emit(self: *Self, event: EventType) void {
            const io = getIo();
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);

            for (self.callbacks.items) |cb| {
                cb(event);
            }
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.callbacks.deinit(allocator);
        }
    };
}
