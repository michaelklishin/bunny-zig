const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

test "connect with default configuration" {
    const _t = h.TestTimer.start("connect with default configuration"); defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();
    try testing.expect(conn.isOpen());
    try testing.expect(conn.negotiated_frame_max > 0);
    try testing.expect(conn.negotiated_channel_max > 0);
    try testing.expect(!conn.isBlocked());
    try testing.expect(conn.blockedReason() == null);
}

test "connect via TLS" {
    const _t = h.TestTimer.start("connect via TLS"); defer _t.stop();
    const certs_env = std.c.getenv("TLS_CERTS_DIR");
    if (certs_env == null) return error.SkipZigTest;
    const certs_dir: []const u8 = std.mem.sliceTo(certs_env.?, 0);

    var ca_path_buf: [4096]u8 = undefined;
    const ca_path = std.fmt.bufPrint(&ca_path_buf, "{s}/ca_certificate.pem", .{certs_dir}) catch return error.SkipZigTest;

    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = 5671,
        .tls = .{
            .host = h.testHost(),
            .ca_file = ca_path,
        },
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    try testing.expect(conn.isOpen());
}

test "connect and close gracefully" {
    const _t = h.TestTimer.start("connect and close gracefully"); defer _t.stop();
    const conn = try h.openTestConnection();
    conn.close();
    try testing.expect(!conn.isOpen());
    conn.deinit();
}
