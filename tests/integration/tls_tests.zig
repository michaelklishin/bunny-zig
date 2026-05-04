const std = @import("std");
const bunny = @import("bunny");
const h = @import("test_helpers.zig");
const testing = h.testing;

// TLS tests rely on a TLS-enabled RabbitMQ node and a CA bundle pointed to by
// `TLS_CERTS_DIR`. Each test skips when the env var is unset so the suite
// stays runnable on machines without a TLS setup.

fn caPath(buf: []u8) ?[]const u8 {
    const certs_dir = std.testing.environ.getPosix("TLS_CERTS_DIR") orelse return null;
    return std.fmt.bufPrint(buf, "{s}/ca_certificate.pem", .{certs_dir}) catch null;
}

test "TLS: skip_peer_certificate_chain_verification connects without a CA bundle" {
    const _t = h.TestTimer.start("TLS: skip_peer_certificate_chain_verification connects without a CA bundle");
    defer _t.stop();
    if (std.testing.environ.getPosix("TLS_CERTS_DIR") == null) return error.SkipZigTest;

    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = 5671,
        .tls = .{
            .host = h.testHost(),
            .skip_peer_certificate_chain_verification = true,
        },
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    try testing.expect(conn.isOpen());
}

test "TLS: wrong hostname is rejected when chain verification is enabled" {
    const _t = h.TestTimer.start("TLS: wrong hostname is rejected when chain verification is enabled");
    defer _t.stop();
    var ca_path_buf: [4096]u8 = undefined;
    const ca_path = caPath(&ca_path_buf) orelse return error.SkipZigTest;

    const result = bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = 5671,
        .tls = .{
            .host = "definitely-not-the-real-host.invalid",
            .ca_file = ca_path,
        },
        .recovery = .{ .enabled = false },
    });
    try testing.expectError(error.TlsHandshakeFailed, result);
}

test "TLS: skip_peer_certificate_chain_verification accepts a wrong hostname" {
    const _t = h.TestTimer.start("TLS: skip_peer_certificate_chain_verification accepts a wrong hostname");
    defer _t.stop();
    if (std.testing.environ.getPosix("TLS_CERTS_DIR") == null) return error.SkipZigTest;

    const conn = try bunny.Connection.open(h.test_allocator, .{
        .host = h.testHost(),
        .port = 5671,
        .tls = .{
            .host = "wildly-wrong.invalid",
            .skip_peer_certificate_chain_verification = true,
        },
        .recovery = .{ .enabled = false },
    });
    defer conn.deinit();
    try testing.expect(conn.isOpen());
}
