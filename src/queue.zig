/// Queue handle: wraps a channel and queue name for convenient operations.
const Channel = @import("channel.zig").Channel;
const BasicProperties = @import("protocol.zig").properties.BasicProperties;
const Delivery = @import("channel.zig").Delivery;
const GetResult = @import("channel.zig").GetResult;
const AckMode = @import("channel.zig").AckMode;

pub const Queue = struct {
    channel: *Channel,
    name: []const u8,
    message_count: u32,
    consumer_count: u32,

    pub fn publish(self: Queue, body: []const u8, props: BasicProperties) !void {
        return self.channel.publishToQueue(self.name, body, props);
    }

    pub fn bind(self: Queue, exchange: []const u8, routing_key: []const u8) !void {
        return self.channel.queueBind(self.name, exchange, routing_key);
    }

    pub fn unbind(self: Queue, exchange: []const u8, routing_key: []const u8) !void {
        return self.channel.queueUnbind(self.name, exchange, routing_key);
    }

    pub fn purge(self: Queue) !u32 {
        return self.channel.queuePurge(self.name);
    }

    pub fn delete(self: Queue) !u32 {
        return self.channel.queueDelete(self.name);
    }

    /// Subscribe with a server-generated consumer tag.
    pub fn subscribe(self: Queue, ack_mode: AckMode) ![]const u8 {
        return self.channel.basicConsume(self.name, ack_mode);
    }

    /// Subscribe with an explicit consumer tag.
    pub fn subscribeWithTag(self: Queue, consumer_tag: []const u8, ack_mode: AckMode) ![]const u8 {
        return self.channel.basicConsumeWithTag(self.name, consumer_tag, ack_mode);
    }

    /// Subscribe with a callback handler and a server-generated consumer tag.
    pub fn subscribeWith(self: Queue, ack_mode: AckMode, handler: *const fn (Delivery) void) ![]const u8 {
        return self.channel.basicConsumeWith(self.name, ack_mode, handler);
    }

    /// Subscribe with a callback handler and an explicit consumer tag.
    pub fn subscribeWithTagAndHandler(self: Queue, consumer_tag: []const u8, ack_mode: AckMode, handler: *const fn (Delivery) void) ![]const u8 {
        return self.channel.basicConsumeWithTagAndHandler(self.name, consumer_tag, ack_mode, handler);
    }

    pub fn get(self: Queue, ack_mode: AckMode) !?GetResult {
        return self.channel.basicGet(self.name, ack_mode);
    }
};
