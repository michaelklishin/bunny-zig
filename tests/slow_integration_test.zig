// Integration tests that need access to local CLI tools.

test {
    _ = @import("integration/connection_blocked_tests.zig");
}
