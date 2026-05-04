/// Queue handle: wraps a channel and queue name for convenient operations.
const std = @import("std");
const Allocator = std.mem.Allocator;
const Channel = @import("channel.zig").Channel;
const BasicProperties = @import("protocol.zig").properties.BasicProperties;
const Delivery = @import("channel.zig").Delivery;
const BasicGetResult = @import("channel.zig").BasicGetResult;
const AckMode = @import("channel.zig").AckMode;

pub const Queue = struct {
    channel: *Channel,
    /// For named queues, aliases the caller's `name`. For server-named queues,
    /// duped from the broker response and freed by `deinit`.
    name: []const u8,
    /// True when `name` was duped (server-named queues only).
    owns_name: bool = false,
    message_count: u32,
    consumer_count: u32,

    /// Free the broker-assigned name for server-named queues. Safe to call
    /// even when the name is borrowed; only frees when the handle owns it.
    pub fn deinit(self: *Queue, allocator: Allocator) void {
        if (self.owns_name) allocator.free(self.name);
        self.* = undefined;
    }

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

    pub fn get(self: Queue, ack_mode: AckMode) !?BasicGetResult {
        return self.channel.basicGet(self.name, ack_mode);
    }
};
