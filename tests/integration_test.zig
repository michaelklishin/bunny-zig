// Integration tests for bunny-zig. Requires a running RabbitMQ node
// on localhost:5672 with default credentials (guest/guest).
//
// Tests are organized by topic under `tests/integration/`. Zig discovers
// `test {}` blocks in transitively imported files, so referencing each
// sub-file here is enough to include it in the test binary.

test {
    _ = @import("integration/connection_tests.zig");
    _ = @import("integration/channel_tests.zig");
    _ = @import("integration/queue_tests.zig");
    _ = @import("integration/exchange_tests.zig");
    _ = @import("integration/binding_tests.zig");
    _ = @import("integration/publish_consume_tests.zig");
    _ = @import("integration/message_properties_tests.zig");
    _ = @import("integration/qos_tests.zig");
    _ = @import("integration/confirm_tests.zig");
    _ = @import("integration/reject_nack_tests.zig");
    _ = @import("integration/tx_tests.zig");
    _ = @import("integration/consumer_tests.zig");
    _ = @import("integration/routing_tests.zig");
    _ = @import("integration/headers_exchange_tests.zig");
    _ = @import("integration/dead_letter_tests.zig");
    _ = @import("integration/recovery_tests.zig");
}
