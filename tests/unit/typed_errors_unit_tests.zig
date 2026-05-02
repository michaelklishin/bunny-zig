const std = @import("std");
const bunny = @import("bunny");
const testing = std.testing;

// Pure mapping tests for `replyCodeToError`. These do not need a broker.

test "replyCodeToError: every AMQP-defined code maps to a distinct typed error" {
    try testing.expectEqual(error.ContentTooLarge, bunny.replyCodeToError(311));
    try testing.expectEqual(error.NoConsumers, bunny.replyCodeToError(313));
    try testing.expectEqual(error.ConnectionForced, bunny.replyCodeToError(320));
    try testing.expectEqual(error.InvalidPath, bunny.replyCodeToError(402));
    try testing.expectEqual(error.AccessRefused, bunny.replyCodeToError(403));
    try testing.expectEqual(error.NotFound, bunny.replyCodeToError(404));
    try testing.expectEqual(error.ResourceLocked, bunny.replyCodeToError(405));
    try testing.expectEqual(error.PreconditionFailed, bunny.replyCodeToError(406));
    try testing.expectEqual(error.FrameError, bunny.replyCodeToError(501));
    try testing.expectEqual(error.SyntaxError, bunny.replyCodeToError(502));
    try testing.expectEqual(error.CommandInvalid, bunny.replyCodeToError(503));
    try testing.expectEqual(error.UnexpectedFrame, bunny.replyCodeToError(505));
    try testing.expectEqual(error.ResourceError, bunny.replyCodeToError(506));
    try testing.expectEqual(error.NotAllowed, bunny.replyCodeToError(530));
    try testing.expectEqual(error.NotImplemented, bunny.replyCodeToError(540));
    try testing.expectEqual(error.InternalError, bunny.replyCodeToError(541));
}

test "replyCodeToError: success and unmapped codes fall back to ChannelClosed" {
    // 200 SUCCESS is the normal close path and has no typed variant.
    try testing.expectEqual(error.ChannelClosed, bunny.replyCodeToError(200));
    // 312 NO_ROUTE arrives as basic.return, never as a channel close, so it
    // is intentionally unmapped here.
    try testing.expectEqual(error.ChannelClosed, bunny.replyCodeToError(312));
    // 504 CHANNEL_ERROR is intentionally unmapped to avoid a name clash with
    // the surrounding `ChannelError` set.
    try testing.expectEqual(error.ChannelClosed, bunny.replyCodeToError(504));
    // Vendor-specific or unknown codes fall back as well.
    try testing.expectEqual(error.ChannelClosed, bunny.replyCodeToError(0));
    try testing.expectEqual(error.ChannelClosed, bunny.replyCodeToError(1));
    try testing.expectEqual(error.ChannelClosed, bunny.replyCodeToError(999));
    try testing.expectEqual(error.ChannelClosed, bunny.replyCodeToError(65535));
}

test "replyCodeToError: property: every u16 maps to some error in ChannelError" {
    var code: u32 = 0;
    var unmapped: u32 = 0;
    while (code <= std.math.maxInt(u16)) : (code += 1) {
        if (bunny.replyCodeToError(@intCast(code)) == error.ChannelClosed) unmapped += 1;
    }
    // 16 mapped codes leaves the rest of the u16 space falling back.
    try testing.expectEqual(@as(u32, std.math.maxInt(u16) + 1 - 16), unmapped);
}

test "replyCodeToError: property: mapped codes are pairwise distinct" {
    const mapped_codes = [_]u16{ 311, 313, 320, 402, 403, 404, 405, 406, 501, 502, 503, 505, 506, 530, 540, 541 };
    for (mapped_codes, 0..) |a, i| {
        const ea = bunny.replyCodeToError(a);
        try testing.expect(ea != error.ChannelClosed);
        for (mapped_codes[i + 1 ..]) |b| {
            try testing.expect(ea != bunny.replyCodeToError(b));
        }
    }
}
