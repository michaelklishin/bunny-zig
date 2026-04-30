/// Shared helpers for bunny-zig integration tests.
///
/// All tests require a running RabbitMQ node on localhost:5672 with
/// default credentials (guest/guest).
const std = @import("std");
const bunny = @import("bunny");
const api = @import("rabbitmq_http_api_client");

pub const testing = std.testing;
pub const BasicProperties = bunny.BasicProperties;

pub fn testHost() []const u8 {
    const env = std.c.getenv("BUNNY_ZIG_HOST");
    return if (env) |e| std.mem.sliceTo(e, 0) else "127.0.0.1";
}

pub fn testPort() u16 {
    const env = std.c.getenv("BUNNY_ZIG_PORT");
    const port_str: []const u8 = if (env) |e| std.mem.sliceTo(e, 0) else "5672";
    return std.fmt.parseInt(u16, port_str, 10) catch 5672;
}

// Use page_allocator for integration tests: the wire protocol decodes nested
// field tables that are owned by the connection. std.testing.allocator's leak
// detection flags these as leaks during connection cleanup.
pub const test_allocator = std.heap.page_allocator;

pub fn sleepMs(ms: u64) void {
    const io = std.Io.Threaded.global_single_threaded.io();
    io.sleep(.{ .nanoseconds = ms * std.time.ns_per_ms }, .boot) catch {};
}

pub fn openTestConnection() !*bunny.Connection {
    return bunny.Connection.open(test_allocator, .{
        .host = testHost(),
        .port = testPort(),
        .recovery = .{ .enabled = false },
    });
}

pub const TestTimer = struct {
    name: []const u8,
    start_ns: i64,

    pub fn start(name: []const u8) TestTimer {
        const io = std.Io.Threaded.global_single_threaded.io();
        return .{ .name = name, .start_ns = @intCast(std.Io.Clock.awake.now(io).nanoseconds) };
    }

    pub fn stop(self: TestTimer) void {
        const io = std.Io.Threaded.global_single_threaded.io();
        const now_ns: i64 = @intCast(std.Io.Clock.awake.now(io).nanoseconds);
        const elapsed_ms = @as(u64, @intCast(@divTrunc(now_ns - self.start_ns, std.time.ns_per_ms)));
        std.debug.print("[test_timing] {s}: {d}ms\n", .{ self.name, elapsed_ms });
    }
};

/// Poll basic.get until a message arrives or attempts are exhausted.
pub fn pollBasicGet(ch: *bunny.Channel, queue: []const u8) !?bunny.GetResult {
    for (0..100) |_| {
        if (try ch.basicGet(queue, .manual)) |result| {
            return result;
        }
        sleepMs(50);
    }
    return null;
}

pub fn openHttpApiClient() !api.Client {
    const io = std.Io.Threaded.global_single_threaded.io();
    return api.Client.init(std.heap.c_allocator, io, .{});
}

/// Force-close a specific connection via the HTTP API,
/// identified by its client-provided connection name.
pub fn forceCloseConnection(http_client: *api.Client, connection_name: []const u8) void {
    sleepMs(1200);
    const conns = (http_client.listConnections() catch return).value;
    for (conns) |ci| {
        const cp = ci.client_properties orelse continue;
        const cn = cp.connection_name orelse continue;
        if (std.mem.eql(u8, cn, connection_name)) {
            http_client.closeConnection(ci.name, "closed by bunny-zig tests", true) catch {};
        }
    }
    sleepMs(500);
}
