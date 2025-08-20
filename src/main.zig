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

    var context = try Context.init();

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
        if (entry.key_len == 0) continue;
        const location = entry.key[0..entry.key_len];
        const stats = entry.value;
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
            .count = 0,
        };
    }

    fn merge(self: *Stats, other: Stats) void {
        self.min = @min(self.min, other.min);
        self.max = @max(self.max, other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    fn add(self: *Stats, temperature: f32) void {
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

    pub fn init() !Context {
        const stats = StatsHashMap.init();
        return Context{
            .stats = stats,
        };
    }
};

fn run_worker(
    context: *Context,
    mutex: *std.Thread.Mutex,
    wait_group: *std.Thread.WaitGroup,
    chunk: []const u8,
) void {
    defer wait_group.finish();

    var worker_context = Context.init() catch unreachable;

    var pos: usize = 0;
    while (pos < chunk.len) {
        const stats = worker_context.stats.get_or_put(chunk, &pos);
        const temperature = parse_float(chunk, &pos);

        stats.add(temperature);
    }

    const entries = worker_context.stats.get_entries();
    for (entries) |entry| {
        if (entry.key_len == 0) continue;

        const location = entry.key[0..entry.key_len];
        const stats = entry.value;

        mutex.lock();

        if (context.stats.get(location)) |existing_stats| {
            existing_stats.merge(stats);
        } else {
            context.stats.put(location, stats);
        }

        mutex.unlock();
    }
}

fn parse_float(chunk: []const u8, pos: *usize) f32 {
    var number: i32 = 0;
    var negative = false;

    for (0..float_len_max) |i| {
        const c = chunk[pos.* + i];
        switch (c) {
            '0'...'9' => number = number * 10 + (c - '0'),
            '-' => negative = true,
            '\n' => {
                pos.* += i + 1;
                break;
            },
            else => {},
        }
    }

    number = if (negative) -number else number;
    return @as(f32, @floatFromInt(number)) / 10;
}

fn sort_locations(_: void, a: StatsHashMap.Entry, b: StatsHashMap.Entry) bool {
    return std.mem.order(u8, &a.key, &b.key) == .lt;
}

const StatsHashMap = struct {
    const Entry = struct {
        key: [128]u8,
        key_len: u8,
        value: Stats,
        hash: u32,
    };

    entries: []Entry,
    key_buffer: [128]u8,
    key_len: u8 = 0,

    const prime: u32 = 0x01000193;
    const offset: u32 = 0x811c9dc5;

    pub fn init() StatsHashMap {
        var entries_list = [_]Entry{
            .{
                .key = undefined,
                .key_len = 0,
                .value = Stats.init(0.0),
                .hash = 0,
            },
        } ** entries_size;
        return StatsHashMap{
            .key_buffer = [_]u8{0} ** 128,
            .entries = &entries_list,
        };
    }

    pub fn get(self: *StatsHashMap, key: []const u8) ?*Stats {
        const hash = StatsHashMap.get_hash(key);
        var index = hash & (entries_size - 1);
        var entry = &self.entries[index];

        while (entry.key_len != 0) {
            if (entry.hash == hash) {
                return &entry.value;
            }
            index = (index + 1) & (entries_size - 1);
            entry = &self.entries[index];
        }

        return null;
    }

    pub fn get_or_put(self: *StatsHashMap, chunk: []const u8, position: *usize) *Stats {
        const key = self.parse_key(chunk, position);

        if (self.get(key)) |existing_stats| {
            return existing_stats;
        }

        const hash = StatsHashMap.get_hash(key);
        const index = hash & (entries_size - 1);
        var entry = &self.entries[index];

        @memcpy(entry.key[0..key.len], key);
        entry.key_len = @intCast(key.len);
        entry.hash = hash;

        return &entry.value;
    }

    pub fn put(self: *StatsHashMap, key: []const u8, value: Stats) void {
        const hash = StatsHashMap.get_hash(key);
        var index = hash & (entries_size - 1);
        var entry = &self.entries[index];

        while (entry.key_len != 0) {
            if (entry.hash == hash) {
                entry.value.merge(value);
                return;
            }
            index = (index + 1) & (entries_size - 1);
            entry = &self.entries[index];
        }

        @memcpy(entry.key[0..key.len], key);
        entry.key_len = @intCast(key.len);
        entry.value = value;
        entry.hash = hash;
    }

    pub fn get_entries(self: *StatsHashMap) []Entry {
        return self.entries;
    }

    fn get_hash(key: []const u8) u32 {
        var hash: u32 = offset;
        for (key) |b| {
            hash ^= b;
            hash *%= prime;
        }
        return hash;
    }

    fn parse_key(self: *StatsHashMap, chunk: []const u8, position: *usize) []const u8 {
        for (0..location_max_len) |i| {
            const c = chunk[position.* + i];
            if (c == ';') {
                position.* += i + 1;
                self.key_len = @intCast(i);
                break;
            }
            self.key_buffer[i] = c;
        }
        return self.key_buffer[0..self.key_len];
    }
};
