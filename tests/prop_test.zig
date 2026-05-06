// Property-based tests, driven by `proptest-zig`. Kept separate from
// the example-based unit tests so the proptest dependency stays out of
// the default unit test build.

test {
    _ = @import("prop/protocol_wire_prop_tests.zig");
    _ = @import("prop/protocol_framing_prop_tests.zig");
    _ = @import("prop/protocol_parser_prop_tests.zig");
    _ = @import("prop/recovery_topology_registry_prop_tests.zig");
}
