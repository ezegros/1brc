const std = @import("std");

var gpa = std.heap.c_allocator;

const float_len_max = 6;
const entries_size: u64 = 1 << 14;
const location_max_len: usize = 100;

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
    defer context.deinit(gpa);

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

    const entries = context.stats.get_entries();

    std.mem.sortUnstable(
        StatsHashMap.Entry,
        entries,
        {},
        sort_locations,
    );

    for (entries) |entry| {
        if (entry.key.len == 0) continue;
        const stats = entry.value;

        const sum = @as(f32, @floatFromInt(stats.sum)) / 10.0;
        const min = @as(f32, @floatFromInt(stats.min)) / 10.0;
        const max = @as(f32, @floatFromInt(stats.max)) / 10.0;
        const count = @as(f32, @floatFromInt(stats.count));
        const avg = sum / count;

        std.debug.print(
            "{s};{d:.1};{d:.1};{d:.1}\n",
            .{ entry.key, min, avg, max },
        );
    }
}

const Stats = struct {
    min: i64,
    max: i64,
    sum: i64,
    count: u32,

    fn init() Stats {
        return .{
            .min = 0,
            .max = 0,
            .sum = 0,
            .count = 0,
        };
    }

    fn merge(self: *Stats, other: Stats) void {
        self.min = @min(self.min, other.min);
        self.max = @max(self.max, other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn add(self: *Stats, temperature: i64) void {
        if (self.count == 0) {
            self.min = temperature;
            self.max = temperature;
            self.sum = temperature;
            self.count = 1;
            return;
        }

        self.min = @min(self.min, temperature);
        self.max = @max(self.max, temperature);
        self.sum += temperature;
        self.count += 1;
    }
};

const Context = struct {
    stats: StatsHashMap,

    pub fn init(allocator: std.mem.Allocator) !Context {
        const stats = StatsHashMap.init(allocator);
        return Context{
            .stats = stats,
        };
    }

    pub fn deinit(self: *Context, allocator: std.mem.Allocator) void {
        self.stats.deinit(allocator);
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
    defer worker_context.deinit(gpa);

    var pos: usize = 0;
    while (pos < chunk.len) {
        const stats = worker_context.stats.get_or_put(chunk, &pos);
        const temperature = parse_number(chunk, &pos);

        stats.add(temperature);
    }

    const entries = worker_context.stats.get_entries();
    for (entries) |entry| {
        if (entry.key.len == 0 or entry.key.len > location_max_len) continue;

        mutex.lock();

        if (context.stats.get(entry.hash)) |existing_stats| {
            existing_stats.merge(entry.value);
        } else {
            context.stats.put(entry);
        }

        mutex.unlock();
    }
}

fn parse_number(chunk: []const u8, pos: *usize) i64 {
    const start = pos.*;
    const negative = chunk[start] == '-';
    const offset: usize = if (negative) 1 else 0;

    var number: i64 = 0;

    if (chunk[start + offset + 1] == '.') {
        number = (@as(i64, chunk[start + offset]) - '0') * 10 + (@as(i64, chunk[start + offset + 2]) - '0');
        pos.* = start + offset + 3 + 1;
    } else {
        number = (@as(i64, chunk[start + offset]) - '0') * 100 +
            (@as(i64, chunk[start + offset + 1]) - '0') * 10 +
            (@as(i64, chunk[start + offset + 3]) - '0');
        pos.* = start + offset + 4 + 1;
    }

    return if (negative) -number else number;
}

fn sort_locations(_: void, a: StatsHashMap.Entry, b: StatsHashMap.Entry) bool {
    if (a.key.len == 0) return false;
    if (b.key.len == 0) return true;
    return std.mem.order(u8, a.key, b.key) == .lt;
}

const StatsHashMap = struct {
    const Entry = struct {
        value: Stats,
        key: []const u8,
        hash: u32,
    };

    entries: []Entry,

    const prime: u32 = 0x01000193;
    const offset: u32 = 0x811c9dc5;

    pub fn init(allocator: std.mem.Allocator) StatsHashMap {
        const entries = allocator.alloc(Entry, entries_size) catch unreachable;
        for (entries) |*entry| {
            entry.* = Entry{
                .value = Stats.init(),
                .key = "",
                .hash = 0,
            };
        }
        return StatsHashMap{
            .entries = entries,
        };
    }

    pub fn deinit(self: *StatsHashMap, allocator: std.mem.Allocator) void {
        allocator.free(self.entries);
        self.entries = &.{};
    }

    pub fn get(self: *StatsHashMap, hash: u32) ?*Stats {
        var index = hash & (entries_size - 1);
        while (true) {
            const entry = &self.entries[index];
            if (entry.key.len == 0) return null;
            if (entry.hash == hash) return &entry.value;
            index = (index + 1) & (entries_size - 1);
        }
    }

    pub fn get_or_put(self: *StatsHashMap, chunk: []const u8, position: *usize) *Stats {
        var key_len: u8 = 0;
        var hash: u32 = offset;
        const start = position.*;

        for (0..location_max_len) |i| {
            const c = chunk[position.* + i];
            if (c == ';') {
                position.* += i + 1;
                key_len = @intCast(i);
                break;
            }
            hash ^= c;
            hash *%= prime;
        }

        const key = chunk[start .. start + key_len];

        var index = hash & (entries_size - 1);
        while (true) {
            var entry = &self.entries[index];
            if (entry.key.len == 0) {
                entry.hash = hash;
                entry.key = key;
                return &entry.value;
            }
            if (entry.hash == hash) {
                return &entry.value;
            }
            index = (index + 1) & (entries_size - 1);
        }
    }

    pub fn put(self: *StatsHashMap, new_entry: Entry) void {
        var index = new_entry.hash & (entries_size - 1);
        while (true) {
            const entry = &self.entries[index];
            if (entry.key.len == 0) {
                self.entries[index] = new_entry;
                return;
            }
            index = (index + 1) & (entries_size - 1);
        }
    }

    pub fn get_entries(self: *StatsHashMap) []Entry {
        return self.entries;
    }
};
