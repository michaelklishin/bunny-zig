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
    const certs_dir = std.testing.environ.getPosix("TLS_CERTS_DIR") orelse return error.SkipZigTest;

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

test "connect with wrong credentials fails authentication" {
    const _t = h.TestTimer.start("connect with wrong credentials fails authentication"); defer _t.stop();
    const result = bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .username = "definitely-not-a-real-user",
        .password = "definitely-not-the-right-password",
        .recovery = .{ .enabled = false },
    });
    try testing.expectError(error.AuthenticationFailed, result);
}

test "connect with a non-existent virtual host is rejected" {
    const _t = h.TestTimer.start("connect with a non-existent virtual host is rejected"); defer _t.stop();
    const result = bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .virtual_host = "/bunny-zig-no-such-vhost",
        .recovery = .{ .enabled = false },
    });
    // The broker closes the connection with ACCESS_REFUSED; the client
    // surfaces this as either AuthenticationFailed or ConnectionClosed
    // depending on which side wins the race to read the close frame.
    if (result) |conn| {
        conn.deinit();
        try testing.expect(false);
    } else |err| switch (err) {
        error.AuthenticationFailed, error.ConnectionClosed => {},
        else => return err,
    }
}

test "connection.update-secret round-trips with the broker" {
    const _t = h.TestTimer.start("connection.update-secret round-trips with the broker");
    defer _t.stop();
    const conn = try h.openTestConnection();
    defer conn.deinit();

    // The default user is not OAuth-backed, so update-secret is a no-op the
    // broker accepts. The contract under test is that the round-trip completes
    // without an exception and the connection stays open.
    try conn.updateSecret("rotated", "scheduled rotation");
    try testing.expect(conn.isOpen());
}

test "negotiate a custom heartbeat interval" {
    const _t = h.TestTimer.start("negotiate a custom heartbeat interval"); defer _t.stop();
    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = h.testPort(),
        .heartbeat = 30,
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    // The negotiated value is min(client, server). Either the client value
    // wins or the server proposes a lower one, so the result is bounded.
    try testing.expect(conn.negotiated_heartbeat > 0);
    try testing.expect(conn.negotiated_heartbeat <= 30);
}
