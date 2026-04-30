/// A simple counter-based notification primitive.
///
/// Unlike `std.Io.Condition`, `signal()` and `broadcast()` always increment
/// the counter and call `futexWake`, regardless of whether any waiters are
/// currently parked. This avoids the lost-wakeup bug that occurs when callers
/// drive the futex directly with `futexWaitTimeout` while bypassing
/// `Condition.wait()`'s internal waiter accounting.
const std = @import("std");
const Io = std.Io;
const Mutex = Io.Mutex;

pub const Notify = struct {
    counter: std.atomic.Value(u32) = .init(0),

    pub const init: Notify = .{};

    pub fn signal(self: *Notify, io: Io) void {
        _ = self.counter.fetchAdd(1, .release);
        io.futexWake(u32, &self.counter.raw, 1);
    }

    pub fn broadcast(self: *Notify, io: Io) void {
        _ = self.counter.fetchAdd(1, .release);
        io.futexWake(u32, &self.counter.raw, std.math.maxInt(u32));
    }

    /// Caller must hold `mutex`. Returns the current counter value.
    pub fn snapshot(self: *Notify) u32 {
        return self.counter.load(.acquire);
    }

    /// Wait for the counter to change from `expected` or for the timeout to elapse.
    /// Caller must NOT hold the mutex during this call.
    pub fn waitTimeout(self: *Notify, io: Io, expected: u32, timeout: Io.Timeout) void {
        io.futexWaitTimeout(u32, &self.counter.raw, expected, timeout) catch {};
    }

    /// Atomically unlock `mutex`, wait for a signal, then re-acquire `mutex`.
    pub fn waitUncancelable(self: *Notify, io: Io, mutex: *Mutex) void {
        const expected = self.snapshot();
        mutex.unlock(io);
        defer mutex.lockUncancelable(io);
        io.futexWaitUncancelable(u32, &self.counter.raw, expected);
    }
};
