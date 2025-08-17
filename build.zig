const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});

    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "obrc",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.linkLibC();

    const sample = b.addExecutable(.{
        .name = "create_sample",
        .root_source_file = null,
        .target = target,
        .optimize = optimize,
    });
    sample.addCSourceFile(.{
        .file = b.path("src/create_sample.c"),
        .flags = &[_][]const u8{
            "-Wall",
            "-Wextra",
            "-Wconversion",
            "-Wformat",
            "-Wimplicit-fallthrough",
            "-Wvla",
        },
    });

    b.installArtifact(exe);
    b.installArtifact(sample);

    const run_cmd = b.addRunArtifact(exe);
    const run_sample = b.addRunArtifact(sample);

    run_cmd.step.dependOn(b.getInstallStep());
    run_sample.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
        run_sample.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    const run_sample_step = b.step("create-sample", "Run the sample app");
    run_sample_step.dependOn(&run_sample.step);
}
