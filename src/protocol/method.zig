/// AMQP 0-9-1 method definitions: encoding and decoding.
const std = @import("std");
const Allocator = std.mem.Allocator;
const c = @import("constants.zig");
const types = @import("types.zig");
const WireBuffer = @import("wire.zig").WireBuffer;
const WireReader = @import("wire.zig").WireReader;
const FieldTable = types.FieldTable;

pub const ClassMethod = struct {
    class_id: u16,
    method_id: u16,
};

/// All supported AMQP 0-9-1 methods.
pub const Method = union(MethodId) {
    // Connection
    connection_start: ConnectionStart,
    connection_start_ok: ConnectionStartOk,
    connection_secure: ConnectionSecure,
    connection_secure_ok: ConnectionSecureOk,
    connection_tune: ConnectionTune,
    connection_tune_ok: ConnectionTuneOk,
    connection_open: ConnectionOpen,
    connection_open_ok: ConnectionOpenOk,
    connection_close: ConnectionClose,
    connection_close_ok: void,
    connection_blocked: ConnectionBlocked,
    connection_unblocked: void,
    connection_update_secret: ConnectionUpdateSecret,
    connection_update_secret_ok: void,

    // Channel
    channel_open: ChannelOpen,
    channel_open_ok: ChannelOpenOk,
    channel_flow: ChannelFlow,
    channel_flow_ok: ChannelFlowOk,
    channel_close: ChannelClose,
    channel_close_ok: void,

    // Exchange
    exchange_declare: ExchangeDeclare,
    exchange_declare_ok: void,
    exchange_delete: ExchangeDelete,
    exchange_delete_ok: void,
    exchange_bind: ExchangeBind,
    exchange_bind_ok: void,
    exchange_unbind: ExchangeUnbind,
    exchange_unbind_ok: void,

    // Queue
    queue_declare: QueueDeclare,
    queue_declare_ok: QueueDeclareOk,
    queue_bind: QueueBind,
    queue_bind_ok: void,
    queue_purge: QueuePurge,
    queue_purge_ok: QueuePurgeOk,
    queue_delete: QueueDelete,
    queue_delete_ok: QueueDeleteOk,
    queue_unbind: QueueUnbind,
    queue_unbind_ok: void,

    // Basic
    basic_qos: BasicQos,
    basic_qos_ok: void,
    basic_consume: BasicConsume,
    basic_consume_ok: BasicConsumeOk,
    basic_cancel: BasicCancel,
    basic_cancel_ok: BasicCancelOk,
    basic_publish: BasicPublish,
    basic_return: BasicReturn,
    basic_deliver: BasicDeliver,
    basic_get: BasicGet,
    basic_get_ok: BasicGetOk,
    basic_get_empty: void,
    basic_ack: BasicAck,
    basic_reject: BasicReject,
    basic_recover_async: BasicRecoverAsync,
    basic_recover: BasicRecover,
    basic_recover_ok: void,
    basic_nack: BasicNack,

    // Confirm
    confirm_select: ConfirmSelect,
    confirm_select_ok: void,

    // Tx
    tx_select: void,
    tx_select_ok: void,
    tx_commit: void,
    tx_commit_ok: void,
    tx_rollback: void,
    tx_rollback_ok: void,

    /// Return the class and method IDs for this method.
    pub fn classAndMethod(self: Method) ClassMethod {
        return self.methodId().classAndMethod();
    }

    pub fn methodId(self: Method) MethodId {
        return @as(MethodId, self);
    }

    /// Encode the method payload (without the class/method header).
    pub fn encode(self: Method, wb: *WireBuffer) void {
        const ids = self.classAndMethod();
        wb.writeU16(ids.class_id);
        wb.writeU16(ids.method_id);

        switch (self) {
            .connection_start_ok => |m| m.encode(wb),
            .connection_secure_ok => |m| m.encode(wb),
            .connection_tune_ok => |m| m.encode(wb),
            .connection_open => |m| m.encode(wb),
            .connection_close => |m| m.encode(wb),
            .connection_update_secret => |m| m.encode(wb),
            .channel_open => |m| m.encode(wb),
            .channel_flow => |m| m.encode(wb),
            .channel_flow_ok => |m| m.encode(wb),
            .channel_close => |m| m.encode(wb),
            .exchange_declare => |m| m.encode(wb),
            .exchange_delete => |m| m.encode(wb),
            .exchange_bind => |m| m.encode(wb),
            .exchange_unbind => |m| m.encode(wb),
            .queue_declare => |m| m.encode(wb),
            .queue_bind => |m| m.encode(wb),
            .queue_purge => |m| m.encode(wb),
            .queue_delete => |m| m.encode(wb),
            .queue_unbind => |m| m.encode(wb),
            .basic_qos => |m| m.encode(wb),
            .basic_consume => |m| m.encode(wb),
            .basic_cancel => |m| m.encode(wb),
            .basic_publish => |m| m.encode(wb),
            .basic_get => |m| m.encode(wb),
            .basic_ack => |m| m.encode(wb),
            .basic_reject => |m| m.encode(wb),
            .basic_recover_async => |m| m.encode(wb),
            .basic_recover => |m| m.encode(wb),
            .basic_nack => |m| m.encode(wb),
            .confirm_select => |m| m.encode(wb),
            // void or server-only methods
            .connection_close_ok,
            .connection_unblocked,
            .connection_update_secret_ok,
            .channel_close_ok,
            .exchange_declare_ok,
            .exchange_delete_ok,
            .exchange_bind_ok,
            .exchange_unbind_ok,
            .queue_bind_ok,
            .queue_unbind_ok,
            .basic_qos_ok,
            .basic_recover_ok,
            .confirm_select_ok,
            .tx_select,
            .tx_select_ok,
            .tx_commit,
            .tx_commit_ok,
            .tx_rollback,
            .tx_rollback_ok,
            => {},
            // Server-to-client only (shouldn't be encoded by client)
            .connection_start,
            .connection_secure,
            .connection_tune,
            .connection_open_ok,
            .connection_blocked,
            .channel_open_ok,
            .basic_consume_ok,
            .basic_cancel_ok,
            .basic_return,
            .basic_deliver,
            .basic_get_ok,
            .basic_get_empty,
            .queue_declare_ok,
            .queue_purge_ok,
            .queue_delete_ok,
            => {},
        }
    }

    /// Decode a method from wire data (after reading class and method IDs).
    pub fn decode(class_id: u16, method_id: u16, reader: *WireReader, allocator: Allocator) !Method {
        const id = MethodId.fromIds(class_id, method_id) orelse return error.UnknownMethod;

        return switch (id) {
            .connection_start => .{ .connection_start = try ConnectionStart.decode(reader, allocator) },
            .connection_start_ok => .{ .connection_start_ok = try ConnectionStartOk.decode(reader, allocator) },
            .connection_secure => .{ .connection_secure = try ConnectionSecure.decode(reader) },
            .connection_secure_ok => .{ .connection_secure_ok = ConnectionSecureOk.decode(reader) },
            .connection_tune => .{ .connection_tune = try ConnectionTune.decode(reader) },
            .connection_tune_ok => .{ .connection_tune_ok = try ConnectionTuneOk.decode(reader) },
            .connection_open => .{ .connection_open = try ConnectionOpen.decode(reader) },
            .connection_open_ok => .{ .connection_open_ok = try ConnectionOpenOk.decode(reader) },
            .connection_close => .{ .connection_close = try ConnectionClose.decode(reader) },
            .connection_close_ok => .{ .connection_close_ok = {} },
            .connection_blocked => .{ .connection_blocked = try ConnectionBlocked.decode(reader) },
            .connection_unblocked => .{ .connection_unblocked = {} },
            .connection_update_secret => .{ .connection_update_secret = try ConnectionUpdateSecret.decode(reader) },
            .connection_update_secret_ok => .{ .connection_update_secret_ok = {} },
            .channel_open => .{ .channel_open = try ChannelOpen.decode(reader) },
            .channel_open_ok => .{ .channel_open_ok = try ChannelOpenOk.decode(reader) },
            .channel_flow => .{ .channel_flow = try ChannelFlow.decode(reader) },
            .channel_flow_ok => .{ .channel_flow_ok = try ChannelFlowOk.decode(reader) },
            .channel_close => .{ .channel_close = try ChannelClose.decode(reader) },
            .channel_close_ok => .{ .channel_close_ok = {} },
            .exchange_declare => .{ .exchange_declare = try ExchangeDeclare.decode(reader, allocator) },
            .exchange_declare_ok => .{ .exchange_declare_ok = {} },
            .exchange_delete => .{ .exchange_delete = try ExchangeDelete.decode(reader) },
            .exchange_delete_ok => .{ .exchange_delete_ok = {} },
            .exchange_bind => .{ .exchange_bind = try ExchangeBind.decode(reader, allocator) },
            .exchange_bind_ok => .{ .exchange_bind_ok = {} },
            .exchange_unbind => .{ .exchange_unbind = try ExchangeUnbind.decode(reader, allocator) },
            .exchange_unbind_ok => .{ .exchange_unbind_ok = {} },
            .queue_declare => .{ .queue_declare = try QueueDeclare.decode(reader, allocator) },
            .queue_declare_ok => .{ .queue_declare_ok = try QueueDeclareOk.decode(reader) },
            .queue_bind => .{ .queue_bind = try QueueBind.decode(reader, allocator) },
            .queue_bind_ok => .{ .queue_bind_ok = {} },
            .queue_purge => .{ .queue_purge = try QueuePurge.decode(reader) },
            .queue_purge_ok => .{ .queue_purge_ok = try QueuePurgeOk.decode(reader) },
            .queue_delete => .{ .queue_delete = try QueueDelete.decode(reader) },
            .queue_delete_ok => .{ .queue_delete_ok = try QueueDeleteOk.decode(reader) },
            .queue_unbind => .{ .queue_unbind = try QueueUnbind.decode(reader, allocator) },
            .queue_unbind_ok => .{ .queue_unbind_ok = {} },
            .basic_qos => .{ .basic_qos = try BasicQos.decode(reader) },
            .basic_qos_ok => .{ .basic_qos_ok = {} },
            .basic_consume => .{ .basic_consume = try BasicConsume.decode(reader, allocator) },
            .basic_consume_ok => .{ .basic_consume_ok = try BasicConsumeOk.decode(reader) },
            .basic_cancel => .{ .basic_cancel = try BasicCancel.decode(reader) },
            .basic_cancel_ok => .{ .basic_cancel_ok = try BasicCancelOk.decode(reader) },
            .basic_publish => .{ .basic_publish = try BasicPublish.decode(reader) },
            .basic_return => .{ .basic_return = try BasicReturn.decode(reader) },
            .basic_deliver => .{ .basic_deliver = try BasicDeliver.decode(reader) },
            .basic_get => .{ .basic_get = try BasicGet.decode(reader) },
            .basic_get_ok => .{ .basic_get_ok = try BasicGetOk.decode(reader) },
            .basic_get_empty => .{ .basic_get_empty = {} },
            .basic_ack => .{ .basic_ack = try BasicAck.decode(reader) },
            .basic_reject => .{ .basic_reject = try BasicReject.decode(reader) },
            .basic_recover_async => .{ .basic_recover_async = try BasicRecoverAsync.decode(reader) },
            .basic_recover => .{ .basic_recover = try BasicRecover.decode(reader) },
            .basic_recover_ok => .{ .basic_recover_ok = {} },
            .basic_nack => .{ .basic_nack = try BasicNack.decode(reader) },
            .confirm_select => .{ .confirm_select = try ConfirmSelect.decode(reader) },
            .confirm_select_ok => .{ .confirm_select_ok = {} },
            .tx_select => .{ .tx_select = {} },
            .tx_select_ok => .{ .tx_select_ok = {} },
            .tx_commit => .{ .tx_commit = {} },
            .tx_commit_ok => .{ .tx_commit_ok = {} },
            .tx_rollback => .{ .tx_rollback = {} },
            .tx_rollback_ok => .{ .tx_rollback_ok = {} },
        };
    }
};

/// Enum of all method IDs for dispatch.
pub const MethodId = enum {
    connection_start,
    connection_start_ok,
    connection_secure,
    connection_secure_ok,
    connection_tune,
    connection_tune_ok,
    connection_open,
    connection_open_ok,
    connection_close,
    connection_close_ok,
    connection_blocked,
    connection_unblocked,
    connection_update_secret,
    connection_update_secret_ok,
    channel_open,
    channel_open_ok,
    channel_flow,
    channel_flow_ok,
    channel_close,
    channel_close_ok,
    exchange_declare,
    exchange_declare_ok,
    exchange_delete,
    exchange_delete_ok,
    exchange_bind,
    exchange_bind_ok,
    exchange_unbind,
    exchange_unbind_ok,
    queue_declare,
    queue_declare_ok,
    queue_bind,
    queue_bind_ok,
    queue_purge,
    queue_purge_ok,
    queue_delete,
    queue_delete_ok,
    queue_unbind,
    queue_unbind_ok,
    basic_qos,
    basic_qos_ok,
    basic_consume,
    basic_consume_ok,
    basic_cancel,
    basic_cancel_ok,
    basic_publish,
    basic_return,
    basic_deliver,
    basic_get,
    basic_get_ok,
    basic_get_empty,
    basic_ack,
    basic_reject,
    basic_recover_async,
    basic_recover,
    basic_recover_ok,
    basic_nack,
    confirm_select,
    confirm_select_ok,
    tx_select,
    tx_select_ok,
    tx_commit,
    tx_commit_ok,
    tx_rollback,
    tx_rollback_ok,

    pub fn classAndMethod(self: MethodId) ClassMethod {
        return switch (self) {
            .connection_start => .{ .class_id = 10, .method_id = 10 },
            .connection_start_ok => .{ .class_id = 10, .method_id = 11 },
            .connection_secure => .{ .class_id = 10, .method_id = 20 },
            .connection_secure_ok => .{ .class_id = 10, .method_id = 21 },
            .connection_tune => .{ .class_id = 10, .method_id = 30 },
            .connection_tune_ok => .{ .class_id = 10, .method_id = 31 },
            .connection_open => .{ .class_id = 10, .method_id = 40 },
            .connection_open_ok => .{ .class_id = 10, .method_id = 41 },
            .connection_close => .{ .class_id = 10, .method_id = 50 },
            .connection_close_ok => .{ .class_id = 10, .method_id = 51 },
            .connection_blocked => .{ .class_id = 10, .method_id = 60 },
            .connection_unblocked => .{ .class_id = 10, .method_id = 61 },
            .connection_update_secret => .{ .class_id = 10, .method_id = 70 },
            .connection_update_secret_ok => .{ .class_id = 10, .method_id = 71 },
            .channel_open => .{ .class_id = 20, .method_id = 10 },
            .channel_open_ok => .{ .class_id = 20, .method_id = 11 },
            .channel_flow => .{ .class_id = 20, .method_id = 20 },
            .channel_flow_ok => .{ .class_id = 20, .method_id = 21 },
            .channel_close => .{ .class_id = 20, .method_id = 40 },
            .channel_close_ok => .{ .class_id = 20, .method_id = 41 },
            .exchange_declare => .{ .class_id = 40, .method_id = 10 },
            .exchange_declare_ok => .{ .class_id = 40, .method_id = 11 },
            .exchange_delete => .{ .class_id = 40, .method_id = 20 },
            .exchange_delete_ok => .{ .class_id = 40, .method_id = 21 },
            .exchange_bind => .{ .class_id = 40, .method_id = 30 },
            .exchange_bind_ok => .{ .class_id = 40, .method_id = 31 },
            .exchange_unbind => .{ .class_id = 40, .method_id = 40 },
            .exchange_unbind_ok => .{ .class_id = 40, .method_id = 51 },
            .queue_declare => .{ .class_id = 50, .method_id = 10 },
            .queue_declare_ok => .{ .class_id = 50, .method_id = 11 },
            .queue_bind => .{ .class_id = 50, .method_id = 20 },
            .queue_bind_ok => .{ .class_id = 50, .method_id = 21 },
            .queue_purge => .{ .class_id = 50, .method_id = 30 },
            .queue_purge_ok => .{ .class_id = 50, .method_id = 31 },
            .queue_delete => .{ .class_id = 50, .method_id = 40 },
            .queue_delete_ok => .{ .class_id = 50, .method_id = 41 },
            .queue_unbind => .{ .class_id = 50, .method_id = 50 },
            .queue_unbind_ok => .{ .class_id = 50, .method_id = 51 },
            .basic_qos => .{ .class_id = 60, .method_id = 10 },
            .basic_qos_ok => .{ .class_id = 60, .method_id = 11 },
            .basic_consume => .{ .class_id = 60, .method_id = 20 },
            .basic_consume_ok => .{ .class_id = 60, .method_id = 21 },
            .basic_cancel => .{ .class_id = 60, .method_id = 30 },
            .basic_cancel_ok => .{ .class_id = 60, .method_id = 31 },
            .basic_publish => .{ .class_id = 60, .method_id = 40 },
            .basic_return => .{ .class_id = 60, .method_id = 50 },
            .basic_deliver => .{ .class_id = 60, .method_id = 60 },
            .basic_get => .{ .class_id = 60, .method_id = 70 },
            .basic_get_ok => .{ .class_id = 60, .method_id = 71 },
            .basic_get_empty => .{ .class_id = 60, .method_id = 72 },
            .basic_ack => .{ .class_id = 60, .method_id = 80 },
            .basic_reject => .{ .class_id = 60, .method_id = 90 },
            .basic_recover_async => .{ .class_id = 60, .method_id = 100 },
            .basic_recover => .{ .class_id = 60, .method_id = 110 },
            .basic_recover_ok => .{ .class_id = 60, .method_id = 111 },
            .basic_nack => .{ .class_id = 60, .method_id = 120 },
            .confirm_select => .{ .class_id = 85, .method_id = 10 },
            .confirm_select_ok => .{ .class_id = 85, .method_id = 11 },
            .tx_select => .{ .class_id = 90, .method_id = 10 },
            .tx_select_ok => .{ .class_id = 90, .method_id = 11 },
            .tx_commit => .{ .class_id = 90, .method_id = 20 },
            .tx_commit_ok => .{ .class_id = 90, .method_id = 21 },
            .tx_rollback => .{ .class_id = 90, .method_id = 30 },
            .tx_rollback_ok => .{ .class_id = 90, .method_id = 31 },
        };
    }

    pub fn fromIds(class_id: u16, method_id: u16) ?MethodId {
        const combined: u32 = @as(u32, class_id) << 16 | method_id;
        return switch (combined) {
            0x000A000A => .connection_start,
            0x000A000B => .connection_start_ok,
            0x000A0014 => .connection_secure,
            0x000A0015 => .connection_secure_ok,
            0x000A001E => .connection_tune,
            0x000A001F => .connection_tune_ok,
            0x000A0028 => .connection_open,
            0x000A0029 => .connection_open_ok,
            0x000A0032 => .connection_close,
            0x000A0033 => .connection_close_ok,
            0x000A003C => .connection_blocked,
            0x000A003D => .connection_unblocked,
            0x000A0046 => .connection_update_secret,
            0x000A0047 => .connection_update_secret_ok,
            0x0014000A => .channel_open,
            0x0014000B => .channel_open_ok,
            0x00140014 => .channel_flow,
            0x00140015 => .channel_flow_ok,
            0x00140028 => .channel_close,
            0x00140029 => .channel_close_ok,
            0x0028000A => .exchange_declare,
            0x0028000B => .exchange_declare_ok,
            0x00280014 => .exchange_delete,
            0x00280015 => .exchange_delete_ok,
            0x0028001E => .exchange_bind,
            0x0028001F => .exchange_bind_ok,
            0x00280028 => .exchange_unbind,
            0x00280033 => .exchange_unbind_ok,
            0x0032000A => .queue_declare,
            0x0032000B => .queue_declare_ok,
            0x00320014 => .queue_bind,
            0x00320015 => .queue_bind_ok,
            0x0032001E => .queue_purge,
            0x0032001F => .queue_purge_ok,
            0x00320028 => .queue_delete,
            0x00320029 => .queue_delete_ok,
            0x00320032 => .queue_unbind,
            0x00320033 => .queue_unbind_ok,
            0x003C000A => .basic_qos,
            0x003C000B => .basic_qos_ok,
            0x003C0014 => .basic_consume,
            0x003C0015 => .basic_consume_ok,
            0x003C001E => .basic_cancel,
            0x003C001F => .basic_cancel_ok,
            0x003C0028 => .basic_publish,
            0x003C0032 => .basic_return,
            0x003C003C => .basic_deliver,
            0x003C0046 => .basic_get,
            0x003C0047 => .basic_get_ok,
            0x003C0048 => .basic_get_empty,
            0x003C0050 => .basic_ack,
            0x003C005A => .basic_reject,
            0x003C0064 => .basic_recover_async,
            0x003C006E => .basic_recover,
            0x003C006F => .basic_recover_ok,
            0x003C0078 => .basic_nack,
            0x0055000A => .confirm_select,
            0x0055000B => .confirm_select_ok,
            0x005A000A => .tx_select,
            0x005A000B => .tx_select_ok,
            0x005A0014 => .tx_commit,
            0x005A0015 => .tx_commit_ok,
            0x005A001E => .tx_rollback,
            0x005A001F => .tx_rollback_ok,
            else => null,
        };
    }
};

//
// Connection method structs
//

pub const ConnectionStart = struct {
    version_major: u8,
    version_minor: u8,
    server_properties: FieldTable,
    mechanisms: []const u8,
    locales: []const u8,

    pub fn decode(reader: *WireReader, allocator: Allocator) !ConnectionStart {
        const major = try reader.readByte();
        const minor = try reader.readByte();
        const result = try FieldTable.decode(reader.rest(), allocator);
        _ = try reader.readBytes(result.consumed);
        const mechanisms = try reader.readLongString();
        const locales = try reader.readLongString();
        return .{
            .version_major = major,
            .version_minor = minor,
            .server_properties = result.table,
            .mechanisms = mechanisms,
            .locales = locales,
        };
    }
};

pub const ConnectionStartOk = struct {
    client_properties: FieldTable,
    mechanism: []const u8,
    response: []const u8,
    locale: []const u8,

    pub fn encode(self: ConnectionStartOk, wb: *WireBuffer) void {
        self.client_properties.encode(wb);
        wb.writeShortString(self.mechanism);
        wb.writeLongString(self.response);
        wb.writeShortString(self.locale);
    }

    pub fn decode(reader: *WireReader, allocator: Allocator) !ConnectionStartOk {
        const result = try FieldTable.decode(reader.rest(), allocator);
        _ = try reader.readBytes(result.consumed);
        return .{
            .client_properties = result.table,
            .mechanism = try reader.readShortString(),
            .response = try reader.readLongString(),
            .locale = try reader.readShortString(),
        };
    }
};

pub const ConnectionSecure = struct {
    challenge: []const u8,

    pub fn decode(reader: *WireReader) !ConnectionSecure {
        return .{ .challenge = try reader.readLongString() };
    }
};

pub const ConnectionSecureOk = struct {
    response: []const u8,

    pub fn encode(self: ConnectionSecureOk, wb: *WireBuffer) void {
        wb.writeLongString(self.response);
    }

    pub fn decode(reader: *WireReader) ConnectionSecureOk {
        return .{ .response = reader.readLongString() catch &.{} };
    }
};

pub const ConnectionTune = struct {
    channel_max: u16,
    frame_max: u32,
    heartbeat: u16,

    pub fn decode(reader: *WireReader) !ConnectionTune {
        return .{
            .channel_max = try reader.readU16(),
            .frame_max = try reader.readU32(),
            .heartbeat = try reader.readU16(),
        };
    }
};

pub const ConnectionTuneOk = struct {
    channel_max: u16,
    frame_max: u32,
    heartbeat: u16,

    pub fn encode(self: ConnectionTuneOk, wb: *WireBuffer) void {
        wb.writeU16(self.channel_max);
        wb.writeU32(self.frame_max);
        wb.writeU16(self.heartbeat);
    }

    pub fn decode(reader: *WireReader) !ConnectionTuneOk {
        return .{
            .channel_max = try reader.readU16(),
            .frame_max = try reader.readU32(),
            .heartbeat = try reader.readU16(),
        };
    }
};

pub const ConnectionOpen = struct {
    virtual_host: []const u8,

    pub fn encode(self: ConnectionOpen, wb: *WireBuffer) void {
        wb.writeShortString(self.virtual_host);
        wb.writeShortString(""); // reserved
        wb.writeByte(0); // reserved
    }

    pub fn decode(reader: *WireReader) !ConnectionOpen {
        const vhost = try reader.readShortString();
        _ = try reader.readShortString(); // reserved
        _ = try reader.readByte(); // reserved
        return .{ .virtual_host = vhost };
    }
};

pub const ConnectionOpenOk = struct {
    known_hosts: []const u8 = "",

    pub fn decode(reader: *WireReader) !ConnectionOpenOk {
        return .{ .known_hosts = try reader.readShortString() };
    }
};

pub const ConnectionClose = struct {
    reply_code: u16,
    reply_text: []const u8,
    class_id: u16,
    method_id: u16,

    pub fn encode(self: ConnectionClose, wb: *WireBuffer) void {
        wb.writeU16(self.reply_code);
        wb.writeShortString(self.reply_text);
        wb.writeU16(self.class_id);
        wb.writeU16(self.method_id);
    }

    pub fn decode(reader: *WireReader) !ConnectionClose {
        return .{
            .reply_code = try reader.readU16(),
            .reply_text = try reader.readShortString(),
            .class_id = try reader.readU16(),
            .method_id = try reader.readU16(),
        };
    }
};

pub const ConnectionBlocked = struct {
    reason: []const u8,

    pub fn decode(reader: *WireReader) !ConnectionBlocked {
        return .{ .reason = try reader.readShortString() };
    }
};

pub const ConnectionUpdateSecret = struct {
    new_secret: []const u8,
    reason: []const u8,

    pub fn encode(self: ConnectionUpdateSecret, wb: *WireBuffer) void {
        wb.writeLongString(self.new_secret);
        wb.writeShortString(self.reason);
    }

    pub fn decode(reader: *WireReader) !ConnectionUpdateSecret {
        return .{
            .new_secret = try reader.readLongString(),
            .reason = try reader.readShortString(),
        };
    }
};

//
// Channel method structs
//

pub const ChannelOpen = struct {
    reserved: []const u8 = "",

    pub fn encode(self: ChannelOpen, wb: *WireBuffer) void {
        wb.writeShortString(self.reserved);
    }

    pub fn decode(reader: *WireReader) !ChannelOpen {
        return .{ .reserved = try reader.readShortString() };
    }
};

pub const ChannelOpenOk = struct {
    reserved: []const u8 = "",

    pub fn decode(reader: *WireReader) !ChannelOpenOk {
        _ = try reader.readLongString(); // reserved
        return .{};
    }
};

pub const ChannelFlow = struct {
    active: bool,

    pub fn encode(self: ChannelFlow, wb: *WireBuffer) void {
        wb.writeByte(if (self.active) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !ChannelFlow {
        return .{ .active = (try reader.readByte()) != 0 };
    }
};

pub const ChannelFlowOk = struct {
    active: bool,

    pub fn encode(self: ChannelFlowOk, wb: *WireBuffer) void {
        wb.writeByte(if (self.active) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !ChannelFlowOk {
        return .{ .active = (try reader.readByte()) != 0 };
    }
};

pub const ChannelClose = struct {
    reply_code: u16,
    reply_text: []const u8,
    class_id: u16,
    method_id: u16,

    pub fn encode(self: ChannelClose, wb: *WireBuffer) void {
        wb.writeU16(self.reply_code);
        wb.writeShortString(self.reply_text);
        wb.writeU16(self.class_id);
        wb.writeU16(self.method_id);
    }

    pub fn decode(reader: *WireReader) !ChannelClose {
        return .{
            .reply_code = try reader.readU16(),
            .reply_text = try reader.readShortString(),
            .class_id = try reader.readU16(),
            .method_id = try reader.readU16(),
        };
    }
};

//
// Exchange method structs
//

pub const ExchangeDeclare = struct {
    reserved1: u16 = 0,
    exchange: []const u8,
    exchange_type: []const u8,
    passive: bool = false,
    durable: bool = false,
    auto_delete: bool = false,
    internal: bool = false,
    no_wait: bool = false,
    arguments: FieldTable = FieldTable.empty,

    pub fn encode(self: ExchangeDeclare, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.exchange);
        wb.writeShortString(self.exchange_type);
        var bits: u8 = 0;
        if (self.passive) bits |= 1;
        if (self.durable) bits |= 2;
        if (self.auto_delete) bits |= 4;
        if (self.internal) bits |= 8;
        if (self.no_wait) bits |= 16;
        wb.writeByte(bits);
        self.arguments.encode(wb);
    }

    pub fn decode(reader: *WireReader, allocator: Allocator) !ExchangeDeclare {
        const reserved1 = try reader.readU16();
        const exchange = try reader.readShortString();
        const exchange_type = try reader.readShortString();
        const bits = try reader.readByte();
        const result = try FieldTable.decode(reader.rest(), allocator);
        _ = try reader.readBytes(result.consumed);
        return .{
            .reserved1 = reserved1,
            .exchange = exchange,
            .exchange_type = exchange_type,
            .passive = bits & 1 != 0,
            .durable = bits & 2 != 0,
            .auto_delete = bits & 4 != 0,
            .internal = bits & 8 != 0,
            .no_wait = bits & 16 != 0,
            .arguments = result.table,
        };
    }
};

pub const ExchangeDelete = struct {
    reserved1: u16 = 0,
    exchange: []const u8,
    if_unused: bool = false,
    no_wait: bool = false,

    pub fn encode(self: ExchangeDelete, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.exchange);
        var bits: u8 = 0;
        if (self.if_unused) bits |= 1;
        if (self.no_wait) bits |= 2;
        wb.writeByte(bits);
    }

    pub fn decode(reader: *WireReader) !ExchangeDelete {
        const reserved1 = try reader.readU16();
        const exchange = try reader.readShortString();
        const bits = try reader.readByte();
        return .{
            .reserved1 = reserved1,
            .exchange = exchange,
            .if_unused = bits & 1 != 0,
            .no_wait = bits & 2 != 0,
        };
    }
};

pub const ExchangeBind = struct {
    reserved1: u16 = 0,
    destination: []const u8,
    source: []const u8,
    routing_key: []const u8 = "",
    no_wait: bool = false,
    arguments: FieldTable = FieldTable.empty,

    pub fn encode(self: ExchangeBind, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.destination);
        wb.writeShortString(self.source);
        wb.writeShortString(self.routing_key);
        wb.writeByte(if (self.no_wait) 1 else 0);
        self.arguments.encode(wb);
    }

    pub fn decode(reader: *WireReader, allocator: Allocator) !ExchangeBind {
        const reserved1 = try reader.readU16();
        const dest = try reader.readShortString();
        const source = try reader.readShortString();
        const rk = try reader.readShortString();
        const bits = try reader.readByte();
        const result = try FieldTable.decode(reader.rest(), allocator);
        _ = try reader.readBytes(result.consumed);
        return .{
            .reserved1 = reserved1,
            .destination = dest,
            .source = source,
            .routing_key = rk,
            .no_wait = bits & 1 != 0,
            .arguments = result.table,
        };
    }
};

pub const ExchangeUnbind = struct {
    reserved1: u16 = 0,
    destination: []const u8,
    source: []const u8,
    routing_key: []const u8 = "",
    no_wait: bool = false,
    arguments: FieldTable = FieldTable.empty,

    pub fn encode(self: ExchangeUnbind, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.destination);
        wb.writeShortString(self.source);
        wb.writeShortString(self.routing_key);
        wb.writeByte(if (self.no_wait) 1 else 0);
        self.arguments.encode(wb);
    }

    pub fn decode(reader: *WireReader, allocator: Allocator) !ExchangeUnbind {
        const reserved1 = try reader.readU16();
        const dest = try reader.readShortString();
        const source = try reader.readShortString();
        const rk = try reader.readShortString();
        const bits = try reader.readByte();
        const result = try FieldTable.decode(reader.rest(), allocator);
        _ = try reader.readBytes(result.consumed);
        return .{
            .reserved1 = reserved1,
            .destination = dest,
            .source = source,
            .routing_key = rk,
            .no_wait = bits & 1 != 0,
            .arguments = result.table,
        };
    }
};

//
// Queue method structs
//

pub const QueueDeclare = struct {
    reserved1: u16 = 0,
    queue: []const u8,
    passive: bool = false,
    durable: bool = false,
    exclusive: bool = false,
    auto_delete: bool = false,
    no_wait: bool = false,
    arguments: FieldTable = FieldTable.empty,

    pub fn encode(self: QueueDeclare, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.queue);
        var bits: u8 = 0;
        if (self.passive) bits |= 1;
        if (self.durable) bits |= 2;
        if (self.exclusive) bits |= 4;
        if (self.auto_delete) bits |= 8;
        if (self.no_wait) bits |= 16;
        wb.writeByte(bits);
        self.arguments.encode(wb);
    }

    pub fn decode(reader: *WireReader, allocator: Allocator) !QueueDeclare {
        const reserved1 = try reader.readU16();
        const queue = try reader.readShortString();
        const bits = try reader.readByte();
        const result = try FieldTable.decode(reader.rest(), allocator);
        _ = try reader.readBytes(result.consumed);
        return .{
            .reserved1 = reserved1,
            .queue = queue,
            .passive = bits & 1 != 0,
            .durable = bits & 2 != 0,
            .exclusive = bits & 4 != 0,
            .auto_delete = bits & 8 != 0,
            .no_wait = bits & 16 != 0,
            .arguments = result.table,
        };
    }
};

pub const QueueDeclareOk = struct {
    queue: []const u8,
    message_count: u32,
    consumer_count: u32,

    pub fn decode(reader: *WireReader) !QueueDeclareOk {
        return .{
            .queue = try reader.readShortString(),
            .message_count = try reader.readU32(),
            .consumer_count = try reader.readU32(),
        };
    }
};

pub const QueueBind = struct {
    reserved1: u16 = 0,
    queue: []const u8,
    exchange: []const u8,
    routing_key: []const u8 = "",
    no_wait: bool = false,
    arguments: FieldTable = FieldTable.empty,

    pub fn encode(self: QueueBind, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.queue);
        wb.writeShortString(self.exchange);
        wb.writeShortString(self.routing_key);
        wb.writeByte(if (self.no_wait) 1 else 0);
        self.arguments.encode(wb);
    }

    pub fn decode(reader: *WireReader, allocator: Allocator) !QueueBind {
        const reserved1 = try reader.readU16();
        const queue = try reader.readShortString();
        const exchange = try reader.readShortString();
        const rk = try reader.readShortString();
        const bits = try reader.readByte();
        const result = try FieldTable.decode(reader.rest(), allocator);
        _ = try reader.readBytes(result.consumed);
        return .{
            .reserved1 = reserved1,
            .queue = queue,
            .exchange = exchange,
            .routing_key = rk,
            .no_wait = bits & 1 != 0,
            .arguments = result.table,
        };
    }
};

pub const QueuePurge = struct {
    reserved1: u16 = 0,
    queue: []const u8,
    no_wait: bool = false,

    pub fn encode(self: QueuePurge, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.queue);
        wb.writeByte(if (self.no_wait) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !QueuePurge {
        return .{
            .reserved1 = try reader.readU16(),
            .queue = try reader.readShortString(),
            .no_wait = (try reader.readByte()) & 1 != 0,
        };
    }
};

pub const QueuePurgeOk = struct {
    message_count: u32,

    pub fn decode(reader: *WireReader) !QueuePurgeOk {
        return .{ .message_count = try reader.readU32() };
    }
};

pub const QueueDelete = struct {
    reserved1: u16 = 0,
    queue: []const u8,
    if_unused: bool = false,
    if_empty: bool = false,
    no_wait: bool = false,

    pub fn encode(self: QueueDelete, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.queue);
        var bits: u8 = 0;
        if (self.if_unused) bits |= 1;
        if (self.if_empty) bits |= 2;
        if (self.no_wait) bits |= 4;
        wb.writeByte(bits);
    }

    pub fn decode(reader: *WireReader) !QueueDelete {
        const reserved1 = try reader.readU16();
        const queue = try reader.readShortString();
        const bits = try reader.readByte();
        return .{
            .reserved1 = reserved1,
            .queue = queue,
            .if_unused = bits & 1 != 0,
            .if_empty = bits & 2 != 0,
            .no_wait = bits & 4 != 0,
        };
    }
};

pub const QueueDeleteOk = struct {
    message_count: u32,

    pub fn decode(reader: *WireReader) !QueueDeleteOk {
        return .{ .message_count = try reader.readU32() };
    }
};

pub const QueueUnbind = struct {
    reserved1: u16 = 0,
    queue: []const u8,
    exchange: []const u8,
    routing_key: []const u8 = "",
    arguments: FieldTable = FieldTable.empty,

    pub fn encode(self: QueueUnbind, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.queue);
        wb.writeShortString(self.exchange);
        wb.writeShortString(self.routing_key);
        self.arguments.encode(wb);
    }

    pub fn decode(reader: *WireReader, allocator: Allocator) !QueueUnbind {
        const reserved1 = try reader.readU16();
        const queue = try reader.readShortString();
        const exchange = try reader.readShortString();
        const rk = try reader.readShortString();
        const result = try FieldTable.decode(reader.rest(), allocator);
        _ = try reader.readBytes(result.consumed);
        return .{
            .reserved1 = reserved1,
            .queue = queue,
            .exchange = exchange,
            .routing_key = rk,
            .arguments = result.table,
        };
    }
};

//
// Basic method structs
//

pub const BasicQos = struct {
    prefetch_size: u32 = 0,
    prefetch_count: u16,
    global: bool = false,

    pub fn encode(self: BasicQos, wb: *WireBuffer) void {
        wb.writeU32(self.prefetch_size);
        wb.writeU16(self.prefetch_count);
        wb.writeByte(if (self.global) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !BasicQos {
        return .{
            .prefetch_size = try reader.readU32(),
            .prefetch_count = try reader.readU16(),
            .global = (try reader.readByte()) != 0,
        };
    }
};

pub const BasicConsume = struct {
    reserved1: u16 = 0,
    queue: []const u8,
    consumer_tag: []const u8 = "",
    no_local: bool = false,
    no_ack: bool = false,
    exclusive: bool = false,
    no_wait: bool = false,
    arguments: FieldTable = FieldTable.empty,

    pub fn encode(self: BasicConsume, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.queue);
        wb.writeShortString(self.consumer_tag);
        var bits: u8 = 0;
        if (self.no_local) bits |= 1;
        if (self.no_ack) bits |= 2;
        if (self.exclusive) bits |= 4;
        if (self.no_wait) bits |= 8;
        wb.writeByte(bits);
        self.arguments.encode(wb);
    }

    pub fn decode(reader: *WireReader, allocator: Allocator) !BasicConsume {
        const reserved1 = try reader.readU16();
        const queue = try reader.readShortString();
        const tag = try reader.readShortString();
        const bits = try reader.readByte();
        const result = try FieldTable.decode(reader.rest(), allocator);
        _ = try reader.readBytes(result.consumed);
        return .{
            .reserved1 = reserved1,
            .queue = queue,
            .consumer_tag = tag,
            .no_local = bits & 1 != 0,
            .no_ack = bits & 2 != 0,
            .exclusive = bits & 4 != 0,
            .no_wait = bits & 8 != 0,
            .arguments = result.table,
        };
    }
};

pub const BasicConsumeOk = struct {
    consumer_tag: []const u8,

    pub fn decode(reader: *WireReader) !BasicConsumeOk {
        return .{ .consumer_tag = try reader.readShortString() };
    }
};

pub const BasicCancel = struct {
    consumer_tag: []const u8,
    no_wait: bool = false,

    pub fn encode(self: BasicCancel, wb: *WireBuffer) void {
        wb.writeShortString(self.consumer_tag);
        wb.writeByte(if (self.no_wait) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !BasicCancel {
        return .{
            .consumer_tag = try reader.readShortString(),
            .no_wait = (try reader.readByte()) & 1 != 0,
        };
    }
};

pub const BasicCancelOk = struct {
    consumer_tag: []const u8,

    pub fn decode(reader: *WireReader) !BasicCancelOk {
        return .{ .consumer_tag = try reader.readShortString() };
    }
};

pub const BasicPublish = struct {
    reserved1: u16 = 0,
    exchange: []const u8 = "",
    routing_key: []const u8 = "",
    mandatory: bool = false,

    pub fn encode(self: BasicPublish, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.exchange);
        wb.writeShortString(self.routing_key);
        var bits: u8 = 0;
        if (self.mandatory) bits |= 1;
        wb.writeByte(bits);
    }

    pub fn decode(reader: *WireReader) !BasicPublish {
        const reserved1 = try reader.readU16();
        const exchange = try reader.readShortString();
        const rk = try reader.readShortString();
        const bits = try reader.readByte();
        return .{
            .reserved1 = reserved1,
            .exchange = exchange,
            .routing_key = rk,
            .mandatory = bits & 1 != 0,
        };
    }
};

pub const BasicReturn = struct {
    reply_code: u16,
    reply_text: []const u8,
    exchange: []const u8,
    routing_key: []const u8,

    pub fn decode(reader: *WireReader) !BasicReturn {
        return .{
            .reply_code = try reader.readU16(),
            .reply_text = try reader.readShortString(),
            .exchange = try reader.readShortString(),
            .routing_key = try reader.readShortString(),
        };
    }
};

pub const BasicDeliver = struct {
    consumer_tag: []const u8,
    delivery_tag: u64,
    redelivered: bool,
    exchange: []const u8,
    routing_key: []const u8,

    pub fn decode(reader: *WireReader) !BasicDeliver {
        return .{
            .consumer_tag = try reader.readShortString(),
            .delivery_tag = try reader.readU64(),
            .redelivered = (try reader.readByte()) != 0,
            .exchange = try reader.readShortString(),
            .routing_key = try reader.readShortString(),
        };
    }
};

pub const BasicGet = struct {
    reserved1: u16 = 0,
    queue: []const u8,
    no_ack: bool = false,

    pub fn encode(self: BasicGet, wb: *WireBuffer) void {
        wb.writeU16(self.reserved1);
        wb.writeShortString(self.queue);
        wb.writeByte(if (self.no_ack) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !BasicGet {
        return .{
            .reserved1 = try reader.readU16(),
            .queue = try reader.readShortString(),
            .no_ack = (try reader.readByte()) & 1 != 0,
        };
    }
};

pub const BasicGetOk = struct {
    delivery_tag: u64,
    redelivered: bool,
    exchange: []const u8,
    routing_key: []const u8,
    message_count: u32,

    pub fn decode(reader: *WireReader) !BasicGetOk {
        return .{
            .delivery_tag = try reader.readU64(),
            .redelivered = (try reader.readByte()) != 0,
            .exchange = try reader.readShortString(),
            .routing_key = try reader.readShortString(),
            .message_count = try reader.readU32(),
        };
    }
};

pub const BasicAck = struct {
    delivery_tag: u64,
    multiple: bool = false,

    pub fn encode(self: BasicAck, wb: *WireBuffer) void {
        wb.writeU64(self.delivery_tag);
        wb.writeByte(if (self.multiple) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !BasicAck {
        return .{
            .delivery_tag = try reader.readU64(),
            .multiple = (try reader.readByte()) & 1 != 0,
        };
    }
};

pub const BasicReject = struct {
    delivery_tag: u64,
    requeue: bool = true,

    pub fn encode(self: BasicReject, wb: *WireBuffer) void {
        wb.writeU64(self.delivery_tag);
        wb.writeByte(if (self.requeue) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !BasicReject {
        return .{
            .delivery_tag = try reader.readU64(),
            .requeue = (try reader.readByte()) & 1 != 0,
        };
    }
};

pub const BasicRecoverAsync = struct {
    requeue: bool = true,

    pub fn encode(self: BasicRecoverAsync, wb: *WireBuffer) void {
        wb.writeByte(if (self.requeue) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !BasicRecoverAsync {
        return .{ .requeue = (try reader.readByte()) & 1 != 0 };
    }
};

pub const BasicRecover = struct {
    requeue: bool = true,

    pub fn encode(self: BasicRecover, wb: *WireBuffer) void {
        wb.writeByte(if (self.requeue) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !BasicRecover {
        return .{ .requeue = (try reader.readByte()) & 1 != 0 };
    }
};

pub const BasicNack = struct {
    delivery_tag: u64,
    multiple: bool = false,
    requeue: bool = true,

    pub fn encode(self: BasicNack, wb: *WireBuffer) void {
        wb.writeU64(self.delivery_tag);
        var bits: u8 = 0;
        if (self.multiple) bits |= 1;
        if (self.requeue) bits |= 2;
        wb.writeByte(bits);
    }

    pub fn decode(reader: *WireReader) !BasicNack {
        const tag = try reader.readU64();
        const bits = try reader.readByte();
        return .{
            .delivery_tag = tag,
            .multiple = bits & 1 != 0,
            .requeue = bits & 2 != 0,
        };
    }
};

//
// Confirm method structs
//

pub const ConfirmSelect = struct {
    no_wait: bool = false,

    pub fn encode(self: ConfirmSelect, wb: *WireBuffer) void {
        wb.writeByte(if (self.no_wait) 1 else 0);
    }

    pub fn decode(reader: *WireReader) !ConfirmSelect {
        return .{ .no_wait = (try reader.readByte()) & 1 != 0 };
    }
};

// Tests
test "method ID lookup roundtrip" {
    const ids = MethodId.connection_start.classAndMethod();
    try std.testing.expectEqual(10, ids.class_id);
    try std.testing.expectEqual(10, ids.method_id);

    const back = MethodId.fromIds(ids.class_id, ids.method_id);
    try std.testing.expectEqual(MethodId.connection_start, back.?);
}

test "all method IDs roundtrip" {
    const all_ids = comptime std.enums.values(MethodId);
    for (all_ids) |id| {
        const cm = id.classAndMethod();
        const back = MethodId.fromIds(cm.class_id, cm.method_id);
        try std.testing.expectEqual(id, back.?);
    }
}

test "basic_publish encode/decode" {
    var buf: [256]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const m = Method{ .basic_publish = .{
        .exchange = "my-exchange",
        .routing_key = "my.key",
        .mandatory = true,
    } };
    m.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const class_id = try reader.readU16();
    const method_id = try reader.readU16();
    const decoded = try Method.decode(class_id, method_id, &reader, std.testing.allocator);

    try std.testing.expectEqualSlices(u8, "my-exchange", decoded.basic_publish.exchange);
    try std.testing.expectEqualSlices(u8, "my.key", decoded.basic_publish.routing_key);
    try std.testing.expect(decoded.basic_publish.mandatory);
}

test "basic_ack encode/decode" {
    var buf: [64]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const m = Method{ .basic_ack = .{ .delivery_tag = 42, .multiple = true } };
    m.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const class_id = try reader.readU16();
    const method_id = try reader.readU16();
    const decoded = try Method.decode(class_id, method_id, &reader, std.testing.allocator);

    try std.testing.expectEqual(42, decoded.basic_ack.delivery_tag);
    try std.testing.expect(decoded.basic_ack.multiple);
}

test "connection_close encode/decode" {
    var buf: [256]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const m = Method{ .connection_close = .{
        .reply_code = 200,
        .reply_text = "Normal shutdown",
        .class_id = 0,
        .method_id = 0,
    } };
    m.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const class_id = try reader.readU16();
    const method_id = try reader.readU16();
    const decoded = try Method.decode(class_id, method_id, &reader, std.testing.allocator);

    try std.testing.expectEqual(200, decoded.connection_close.reply_code);
    try std.testing.expectEqualSlices(u8, "Normal shutdown", decoded.connection_close.reply_text);
}

test "queue_declare encode/decode" {
    const allocator = std.testing.allocator;
    var buf: [256]u8 = undefined;
    var wb = WireBuffer.init(&buf);
    const m = Method{ .queue_declare = .{
        .queue = "test-queue",
        .durable = true,
        .exclusive = false,
        .auto_delete = false,
    } };
    m.encode(&wb);

    var reader = WireReader.init(wb.getWritten());
    const class_id = try reader.readU16();
    const method_id = try reader.readU16();
    const decoded = try Method.decode(class_id, method_id, &reader, allocator);

    try std.testing.expectEqualSlices(u8, "test-queue", decoded.queue_declare.queue);
    try std.testing.expect(decoded.queue_declare.durable);
    try std.testing.expect(!decoded.queue_declare.exclusive);
}
