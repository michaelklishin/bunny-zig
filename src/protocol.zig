/// AMQP 0-9-1 protocol module.
pub const constants = @import("protocol/constants.zig");
pub const wire = @import("protocol/wire.zig");
pub const types = @import("protocol/types.zig");
pub const properties = @import("protocol/properties.zig");
pub const method = @import("protocol/method.zig");
pub const frame = @import("protocol/frame.zig");

pub const WireBuffer = wire.WireBuffer;
pub const WireReader = wire.WireReader;
pub const FieldTable = types.FieldTable;
pub const FieldValue = types.FieldValue;
pub const BasicProperties = properties.BasicProperties;
pub const Method = method.Method;
pub const MethodId = method.MethodId;
pub const Frame = frame.Frame;

test {
    @import("std").testing.refAllDecls(@This());
}
