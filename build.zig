const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const tls_mod = b.dependency("tls", .{}).module("tls");

    const bunny_mod = b.addModule("bunny", .{
        .root_source_file = b.path("src/bunny.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "tls", .module = tls_mod },
        },
    });

    // HTTP API client for integration tests (local path dependency)
    const http_api_mod = b.createModule(.{
        .root_source_file = .{ .cwd_relative = "../rabbitmq-http-api-client-zig.git/src/root.zig" },
        .target = target,
        .optimize = optimize,
    });

    const unit_test_mod = b.createModule(.{
        .root_source_file = b.path("src/bunny.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "tls", .module = tls_mod },
        },
    });
    const unit_tests = b.addTest(.{ .root_module = unit_test_mod });
    const run_unit_tests = b.addRunArtifact(unit_tests);

    const integration_test_mod = b.createModule(.{
        .root_source_file = b.path("tests/integration_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "bunny", .module = bunny_mod },
            .{ .name = "rabbitmq_http_api_client", .module = http_api_mod },
        },
    });
    const integration_tests = b.addTest(.{ .root_module = integration_test_mod });
    const run_integration_tests = b.addRunArtifact(integration_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    const integration_test_step = b.step("integration-test", "Run integration tests (requires RabbitMQ)");
    integration_test_step.dependOn(&run_integration_tests.step);

    const bench_exe = b.addExecutable(.{
        .name = "publish-throughput",
        .root_module = b.createModule(.{
            .root_source_file = b.path("benchmarks/publish_throughput.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "bunny", .module = bunny_mod },
            },
        }),
    });
    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run publish throughput benchmark (requires RabbitMQ)");
    bench_step.dependOn(&run_bench.step);

}
