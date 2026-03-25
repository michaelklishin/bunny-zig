/// Exchange handle: wraps a channel and exchange name for convenient operations.
const Channel = @import("channel.zig").Channel;
const BasicProperties = @import("protocol.zig").properties.BasicProperties;
const PublishOptions = @import("channel.zig").PublishOptions;

pub const Exchange = struct {
    channel: *Channel,
    name: []const u8,

    pub fn publish(self: Exchange, body: []const u8, routing_key: []const u8, props: BasicProperties) !void {
        return self.channel.publish(body, .{
            .exchange = self.name,
            .routing_key = routing_key,
            .properties = props,
        });
    }

    pub fn publishMandatory(self: Exchange, body: []const u8, routing_key: []const u8, props: BasicProperties) !void {
        return self.channel.publish(body, .{
            .exchange = self.name,
            .routing_key = routing_key,
            .mandatory = true,
            .properties = props,
        });
    }

    pub fn bind(self: Exchange, source: []const u8, routing_key: []const u8) !void {
        return self.channel.exchangeBind(self.name, source, routing_key);
    }

    pub fn unbind(self: Exchange, source: []const u8, routing_key: []const u8) !void {
        return self.channel.exchangeUnbind(self.name, source, routing_key);
    }

    pub fn delete(self: Exchange) !void {
        return self.channel.exchangeDelete(self.name);
    }
};
