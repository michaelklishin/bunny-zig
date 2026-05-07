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
    return std.testing.environ.getPosix("BUNNY_ZIG_HOST") orelse "127.0.0.1";
}

pub fn testPort() u16 {
    const port_str = std.testing.environ.getPosix("BUNNY_ZIG_PORT") orelse "5672";
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
pub fn pollBasicGet(ch: *bunny.Channel, queue: []const u8) !?bunny.BasicGetResult {
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
/// identified by its client-provided connection name. Polls until the
/// management API has registered the connection: a fire-and-forget lookup
/// races with the broker and silently no-ops if the connection is not yet
/// listed.
pub fn forceCloseConnection(http_client: *api.Client, connection_name: []const u8) void {
    // ~10 s total budget at 25 ms granularity. The connection may take a moment
    // to show up in the management API after stats are first emitted, so the
    // tight loop polls aggressively rather than relying on a fixed pre-sleep.
    var attempt: u32 = 0;
    while (attempt < 400) : (attempt += 1) {
        const conns = (http_client.listConnections() catch {
            sleepMs(25);
            continue;
        }).value;
        for (conns) |ci| {
            const cp = ci.client_properties orelse continue;
            const cn = cp.connection_name orelse continue;
            if (std.mem.eql(u8, cn, connection_name)) {
                http_client.closeConnection(ci.name, "closed by bunny-zig tests", true) catch {};
                sleepMs(25);
                return;
            }
        }
        sleepMs(25);
    }
}

/// Resolve the rabbitmqctl binary, honoring BUNNY_RABBITMQCTL.
fn rabbitmqctlValue() ?[]const u8 {
    const value = std.testing.environ.getPosix("BUNNY_RABBITMQCTL") orelse return null;
    return if (value.len > 0) value else null;
}

/// Invoke rabbitmqctl with the given arguments. Returns false if the binary
/// is unavailable (BUNNY_RABBITMQCTL unset) or the command failed.
///
/// Accepts the Ruby Bunny convention `DOCKER:<container-id-or-name>`, which
/// rewrites the invocation to `docker exec <container> rabbitmqctl <args...>`.
/// Otherwise the value is treated as a path to the rabbitmqctl binary.
pub fn runRabbitmqctl(args: []const []const u8) bool {
    const ctl = rabbitmqctlValue() orelse return false;

    var argv = std.ArrayList([]const u8).empty;
    defer argv.deinit(test_allocator);

    const docker_prefix = "DOCKER:";
    if (std.mem.startsWith(u8, ctl, docker_prefix)) {
        const container = ctl[docker_prefix.len..];
        if (container.len == 0) return false;
        argv.append(test_allocator, "docker") catch return false;
        argv.append(test_allocator, "exec") catch return false;
        argv.append(test_allocator, container) catch return false;
        argv.append(test_allocator, "rabbitmqctl") catch return false;
    } else {
        argv.append(test_allocator, ctl) catch return false;
    }
    argv.appendSlice(test_allocator, args) catch return false;

    var child = std.process.spawn(testing.io, .{
        .argv = argv.items,
        // rabbitmqctl reads tool-version files from the cwd, so use a neutral one.
        .cwd = .{ .path = "/tmp" },
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    }) catch return false;

    const term = child.wait(testing.io) catch return false;
    return term == .exited and term.exited == 0;
}
