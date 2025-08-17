const std = @import("std");

var gpa = std.heap.c_allocator;

pub fn main() !void {
    var args = try std.process.argsWithAllocator(gpa);
    defer args.deinit();
    _ = args.skip();

    const file_name = args.next() orelse "input.txt";
    const file = try std.fs.cwd().openFile(
        file_name,
        .{ .mode = .read_only },
    );
    defer file.close();

    const mapped_mem = try std.posix.mmap(
        null,
        try file.getEndPos(),
        std.posix.system.PROT.READ,
        .{ .TYPE = .SHARED },
        file.handle,
        0,
    );
    defer std.posix.munmap(mapped_mem);

    var thread_pool: std.Thread.Pool = undefined;
    try thread_pool.init(.{ .allocator = gpa });

    var context = try Context.init(gpa);
    defer context.deinit();

    var mutex: std.Thread.Mutex = .{};

    const worker_count = try std.Thread.getCpuCount() - 1;

    var wait_group: std.Thread.WaitGroup = .{};
    wait_group.startMany(worker_count);

    var chunk_start: usize = 0;

    for (0..worker_count) |i| {
        const end_start = mapped_mem.len / worker_count * (i + 1);
        const end = std.mem.indexOfScalarPos(
            u8,
            mapped_mem,
            end_start,
            '\n',
        ) orelse mapped_mem.len - 1;
        const chunk: []const u8 = mapped_mem[chunk_start .. end + 1];
        try thread_pool.spawn(run_worker, .{
            &context,
            &mutex,
            &wait_group,
            chunk,
        });
        chunk_start = end + 1;
        if (chunk_start >= mapped_mem.len) break;
    }

    thread_pool.waitAndWork(&wait_group);

    std.mem.sortUnstable(
        []const u8,
        context.locations.items,
        {},
        sort_locations,
    );

    for (context.locations.items) |location| {
        const stats = context.stats.get(location) orelse continue;
        const avg = stats.sum / @as(f32, @floatFromInt(stats.count));
        std.debug.print(
            "{s};{d:.1};{d:.1};{d:.1}\n",
            .{ location, stats.min, avg, stats.max },
        );
    }
}

const Stats = struct {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,

    fn init(temperature: f32) Stats {
        return Stats{
            .min = temperature,
            .max = temperature,
            .sum = temperature,
            .count = 1,
        };
    }

    fn merge(self: *Stats, other: Stats) void {
        self.min = @min(self.min, other.min);
        self.max = @max(self.max, other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn add(self: *Stats, temperature: f32) void {
        self.min = @min(self.min, temperature);
        self.max = @max(self.max, temperature);
        self.sum += temperature;
        self.count += 1;
    }
};

const Context = struct {
    stats: std.StringHashMap(Stats),
    locations: std.ArrayList([]const u8),

    pub fn init(allocator: std.mem.Allocator) !Context {
        const stats = std.StringHashMap(Stats).init(allocator);
        const locations = std.ArrayList([]const u8).init(allocator);
        return Context{
            .stats = stats,
            .locations = locations,
        };
    }

    pub fn deinit(self: *Context) void {
        self.stats.deinit();
        self.locations.deinit();
    }
};

fn run_worker(
    context: *Context,
    mutex: *std.Thread.Mutex,
    wait_group: *std.Thread.WaitGroup,
    chunk: []const u8,
) void {
    defer wait_group.finish();

    var worker_context = Context.init(gpa) catch unreachable;
    defer worker_context.deinit();

    var pos: usize = 0;
    while (pos < chunk.len) {
        const line_end = std.mem.indexOfScalarPos(
            u8,
            chunk,
            pos,
            '\n',
        ) orelse chunk.len;

        const line = chunk[pos..line_end];

        pos = line_end + 1;

        if (line.len == 0) continue;

        var parts = std.mem.splitScalar(u8, line, ';');

        const location = parts.next() orelse continue;
        const temperature_str = parts.next() orelse continue;
        const temperature = std.fmt.parseFloat(f32, temperature_str) catch continue;

        const stats = worker_context.stats.getOrPut(location) catch unreachable;

        if (stats.found_existing) {
            stats.value_ptr.add(temperature);
        } else {
            stats.value_ptr.* = Stats.init(temperature);
        }
    }

    var iter = worker_context.stats.iterator();
    while (iter.next()) |entry| {
        const location = entry.key_ptr.*;
        const stats = entry.value_ptr.*;

        mutex.lock();

        if (context.stats.getPtr(location)) |existing_stats| {
            existing_stats.merge(stats);
        } else {
            context.stats.put(location, stats) catch unreachable;
            context.locations.append(location) catch unreachable;
        }

        mutex.unlock();
    }
}

fn sort_locations(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}
