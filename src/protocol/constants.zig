/// AMQP 0-9-1 protocol constants.

// Protocol header: "AMQP" followed by 0, 0, 9, 1
pub const protocol_header = "AMQP\x00\x00\x09\x01";

// Frame types
pub const frame_method: u8 = 1;
pub const frame_header: u8 = 2;
pub const frame_body: u8 = 3;
pub const frame_heartbeat: u8 = 8;
pub const frame_end: u8 = 0xCE;

// Frame header size: type(1) + channel(2) + size(4)
pub const frame_header_size: usize = 7;
// Overhead per frame: header + frame_end
pub const frame_overhead: usize = frame_header_size + 1;

// Default negotiation values
pub const default_frame_max: u32 = 131_072;
pub const default_channel_max: u16 = 2047;
pub const default_heartbeat: u16 = 60;
pub const default_port: u16 = 5672;
pub const default_tls_port: u16 = 5671;

// Class IDs
pub const class_connection: u16 = 10;
pub const class_channel: u16 = 20;
pub const class_exchange: u16 = 40;
pub const class_queue: u16 = 50;
pub const class_basic: u16 = 60;
pub const class_confirm: u16 = 85;
pub const class_tx: u16 = 90;

// Connection method IDs
pub const method_connection_start: u16 = 10;
pub const method_connection_start_ok: u16 = 11;
pub const method_connection_secure: u16 = 20;
pub const method_connection_secure_ok: u16 = 21;
pub const method_connection_tune: u16 = 30;
pub const method_connection_tune_ok: u16 = 31;
pub const method_connection_open: u16 = 40;
pub const method_connection_open_ok: u16 = 41;
pub const method_connection_close: u16 = 50;
pub const method_connection_close_ok: u16 = 51;
pub const method_connection_blocked: u16 = 60;
pub const method_connection_unblocked: u16 = 61;
pub const method_connection_update_secret: u16 = 70;
pub const method_connection_update_secret_ok: u16 = 71;

// Channel method IDs
pub const method_channel_open: u16 = 10;
pub const method_channel_open_ok: u16 = 11;
pub const method_channel_flow: u16 = 20;
pub const method_channel_flow_ok: u16 = 21;
pub const method_channel_close: u16 = 40;
pub const method_channel_close_ok: u16 = 41;

// Exchange method IDs
pub const method_exchange_declare: u16 = 10;
pub const method_exchange_declare_ok: u16 = 11;
pub const method_exchange_delete: u16 = 20;
pub const method_exchange_delete_ok: u16 = 21;
pub const method_exchange_bind: u16 = 30;
pub const method_exchange_bind_ok: u16 = 31;
pub const method_exchange_unbind: u16 = 40;
pub const method_exchange_unbind_ok: u16 = 51;

// Queue method IDs
pub const method_queue_declare: u16 = 10;
pub const method_queue_declare_ok: u16 = 11;
pub const method_queue_bind: u16 = 20;
pub const method_queue_bind_ok: u16 = 21;
pub const method_queue_purge: u16 = 30;
pub const method_queue_purge_ok: u16 = 31;
pub const method_queue_delete: u16 = 40;
pub const method_queue_delete_ok: u16 = 41;
pub const method_queue_unbind: u16 = 50;
pub const method_queue_unbind_ok: u16 = 51;

// Basic method IDs
pub const method_basic_qos: u16 = 10;
pub const method_basic_qos_ok: u16 = 11;
pub const method_basic_consume: u16 = 20;
pub const method_basic_consume_ok: u16 = 21;
pub const method_basic_cancel: u16 = 30;
pub const method_basic_cancel_ok: u16 = 31;
pub const method_basic_publish: u16 = 40;
pub const method_basic_return: u16 = 50;
pub const method_basic_deliver: u16 = 60;
pub const method_basic_get: u16 = 70;
pub const method_basic_get_ok: u16 = 71;
pub const method_basic_get_empty: u16 = 72;
pub const method_basic_ack: u16 = 80;
pub const method_basic_reject: u16 = 90;
pub const method_basic_recover_async: u16 = 100;
pub const method_basic_recover: u16 = 110;
pub const method_basic_recover_ok: u16 = 111;
pub const method_basic_nack: u16 = 120;

// Confirm method IDs
pub const method_confirm_select: u16 = 10;
pub const method_confirm_select_ok: u16 = 11;

// Tx method IDs
pub const method_tx_select: u16 = 10;
pub const method_tx_select_ok: u16 = 11;
pub const method_tx_commit: u16 = 20;
pub const method_tx_commit_ok: u16 = 21;
pub const method_tx_rollback: u16 = 30;
pub const method_tx_rollback_ok: u16 = 31;

// AMQP reply codes
pub const ReplyCode = enum(u16) {
    success = 200,
    // Soft errors (channel-level)
    content_too_large = 311,
    no_route = 312,
    no_consumers = 313,
    access_refused = 403,
    not_found = 404,
    resource_locked = 405,
    precondition_failed = 406,
    // Hard errors (connection-level)
    connection_forced = 320,
    invalid_path = 402,
    frame_error = 501,
    syntax_error = 502,
    command_invalid = 503,
    channel_error = 504,
    unexpected_frame = 505,
    resource_error = 506,
    not_allowed = 530,
    not_implemented = 540,
    internal_error = 541,
    _,

    pub fn isSoftError(self: ReplyCode) bool {
        const code = @intFromEnum(self);
        return code >= 300 and code < 500;
    }

    pub fn isHardError(self: ReplyCode) bool {
        const code = @intFromEnum(self);
        return code >= 500;
    }
};

// Delivery modes
pub const delivery_mode_transient: u8 = 1;
pub const delivery_mode_persistent: u8 = 2;

// Basic properties flag bits
pub const prop_content_type: u16 = 0x8000;
pub const prop_content_encoding: u16 = 0x4000;
pub const prop_headers: u16 = 0x2000;
pub const prop_delivery_mode: u16 = 0x1000;
pub const prop_priority: u16 = 0x0800;
pub const prop_correlation_id: u16 = 0x0400;
pub const prop_reply_to: u16 = 0x0200;
pub const prop_expiration: u16 = 0x0100;
pub const prop_message_id: u16 = 0x0080;
pub const prop_timestamp: u16 = 0x0040;
pub const prop_type: u16 = 0x0020;
pub const prop_user_id: u16 = 0x0010;
pub const prop_app_id: u16 = 0x0008;
pub const prop_cluster_id: u16 = 0x0004;

// Client identity
pub const product = "bunny-zig";
pub const version = "0.1.0";
pub const platform = "Zig";
