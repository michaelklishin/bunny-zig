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

    const http_api_dep = b.dependency("rabbitmq_http_api_client", .{
        .target = target,
        .optimize = optimize,
    });
    const http_api_mod = http_api_dep.module("rabbitmq_http_api_client");

    const proptest_dep = b.dependency("proptest", .{
        .target = target,
        .optimize = optimize,
    });
    const proptest_mod = proptest_dep.module("proptest");

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

    const tests_unit_mod = b.createModule(.{
        .root_source_file = b.path("tests/unit_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "bunny", .module = bunny_mod },
        },
    });
    const tests_unit = b.addTest(.{ .root_module = tests_unit_mod });
    const run_tests_unit = b.addRunArtifact(tests_unit);

    const tests_prop_mod = b.createModule(.{
        .root_source_file = b.path("tests/prop_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "bunny", .module = bunny_mod },
            .{ .name = "proptest", .module = proptest_mod },
        },
    });
    const tests_prop = b.addTest(.{ .root_module = tests_prop_mod });
    const run_tests_prop = b.addRunArtifact(tests_prop);

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

    const slow_integration_test_mod = b.createModule(.{
        .root_source_file = b.path("tests/slow_integration_test.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "bunny", .module = bunny_mod },
            .{ .name = "rabbitmq_http_api_client", .module = http_api_mod },
        },
    });
    const slow_integration_tests = b.addTest(.{ .root_module = slow_integration_test_mod });
    const run_slow_integration_tests = b.addRunArtifact(slow_integration_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_tests_unit.step);

    const prop_test_step = b.step("prop-test", "Run property-based tests (proptest-zig)");
    prop_test_step.dependOn(&run_tests_prop.step);

    const integration_test_step = b.step("integration-test", "Run integration tests (requires RabbitMQ)");
    integration_test_step.dependOn(&run_integration_tests.step);

    const slow_integration_test_step = b.step("slow-integration-test", "Run integration tests that need rabbitmqctl (BUNNY_RABBITMQCTL must be set)");
    slow_integration_test_step.dependOn(&run_slow_integration_tests.step);

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
    if (b.args) |args| run_bench.addArgs(args);
    const bench_step = b.step("bench", "Run publish throughput benchmark (requires RabbitMQ)");
    bench_step.dependOn(&run_bench.step);

}
