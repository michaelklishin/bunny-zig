// Unit tests that import the bunny module but do not require a running broker.
// Inline `test {}` blocks under `src/` are still run by `zig build test`; this
// aggregator collects broker-free tests that live under `tests/unit/`.

test {
    _ = @import("unit/typed_errors_unit_tests.zig");
}
