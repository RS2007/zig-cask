const std = @import("std");

const Log = struct {
    //crc: u32, For now we don't need any data corruption detection
    tstamp: i64,
    keysz: usize,
    valuesz: usize,
    key: []const u8,
    value: []const u8,
    const Self = @This();
    pub fn serialize(self: Self, allocator: std.mem.Allocator) []u8 {
        const len = @sizeOf(Self);
        var buf = try std.ArrayList(u8).initCapacity(allocator, len);
        buf.writer().writeStruct(self);
        return (try buf.toOwnedSlice());
    }
    pub fn writeSerialized(log: Self, allocator: std.mem.Allocator) ![]u8 {
        var buf = try std.ArrayList(u8).initCapacity(allocator, @sizeOf(Log));
        try buf.writer().writeInt(i64, log.tstamp, std.builtin.Endian.little);
        try buf.writer().writeInt(usize, log.keysz, std.builtin.Endian.little);
        try buf.writer().writeInt(usize, log.valuesz, std.builtin.Endian.little);
        try buf.writer().writeAll(log.key);
        try buf.writer().writeAll(log.value);
        return (try buf.toOwnedSlice());
    }
};

const BitcaskOpt = enum {
    RW,
    SYNC_ON_PUT,
};

const KeyEntry = struct {
    timestamp: i64,
    offset: usize,
    totalSize: usize,

    pub fn getLog(self: *KeyEntry, handle: *BitcaskHandle) !Log {
        try handle.file.seekTo(self.offset);
        const reader = handle.file.reader();
        const timestamp = try reader.readInt(i64, std.builtin.Endian.little);
        const keysz = try reader.readInt(usize, std.builtin.Endian.little);
        const valuesz = try reader.readInt(usize, std.builtin.Endian.little);
        const key = try handle.allocator.alloc(u8, keysz);
        _ = try reader.readAtLeast(key, keysz);
        const value = try handle.allocator.alloc(u8, valuesz);
        _ = try reader.readAtLeast(value, valuesz);
        return Log{
            .tstamp = timestamp,
            .keysz = keysz,
            .valuesz = valuesz,
            .key = key,
            .value = value,
        };
    }
    pub fn getValue(self: *KeyEntry, handle: *BitcaskHandle) ![]u8 {
        try handle.file.seekTo(self.offset + @sizeOf(i64));
        const keysz = try handle.file.reader().readInt(usize, std.builtin.Endian.little);
        const valuesz = try handle.file.reader().readInt(usize, std.builtin.Endian.little);
        try handle.file.seekTo(self.offset + @sizeOf(i64) + 2 * @sizeOf(usize) + keysz);
        const buf = try handle.allocator.alloc(u8, valuesz);
        _ = try handle.file.reader().readAtLeast(buf, valuesz);
        return buf;
    }
};

const BitcaskHandle = struct {
    file: std.fs.File,
    fileName: []const u8,
    inMemMap: std.StringHashMap(KeyEntry),
    allocator: std.mem.Allocator,
    offset: usize = 0,
    const Self = @This();
    pub fn open(path: []const u8, opt: BitcaskOpt, allocator: std.mem.Allocator) !*BitcaskHandle {
        _ = opt;
        _ = std.fs.cwd().createFile(path, .{}) catch unreachable;
        const file = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch unreachable;
        const caskHandle = try allocator.create(Self);
        const inMemMap = std.StringHashMap(KeyEntry).init(allocator);
        caskHandle.* = Self{
            .file = file,
            .inMemMap = inMemMap,
            .allocator = allocator,
            .fileName = path,
        };
        return caskHandle;
    }

    pub fn openExisting(path: []const u8, allocator: std.mem.Allocator) !*BitcaskHandle {
        const file = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch unreachable;
        var reader = file.reader();
        var inMemMap = std.StringHashMap(KeyEntry).init(allocator);
        var offset: usize = 0;
        while (true) {
            const tStamp = reader.readInt(i64, std.builtin.Endian.little) catch |err| {
                if (err == error.EndOfStream) {
                    break;
                } else {
                    unreachable;
                }
            };
            const keysz = reader.readInt(usize, std.builtin.Endian.little) catch |err| {
                if (err == error.EndOfStream) {
                    break;
                } else {
                    unreachable;
                }
            };
            const valuesz = reader.readInt(usize, std.builtin.Endian.little) catch |err| {
                if (err == error.EndOfStream) {
                    break;
                } else {
                    unreachable;
                }
            };
            const key = try allocator.alloc(u8, keysz);
            _ = try reader.readAtLeast(key, keysz);
            _ = try reader.skipBytes(valuesz, .{});
            try inMemMap.put(key, KeyEntry{
                .offset = offset,
                .totalSize = @sizeOf(i64) + 2 * @sizeOf(usize) + keysz + valuesz,
                .timestamp = tStamp,
            });
            offset += @sizeOf(i64) + 2 * @sizeOf(usize) + keysz + valuesz;
        }
        const caskHandle = try allocator.create(BitcaskHandle);
        caskHandle.* = BitcaskHandle{
            .file = file,
            .inMemMap = inMemMap,
            .allocator = allocator,
            .fileName = path,
        };
        return caskHandle;
    }

    pub fn get(self: *Self, key: []const u8) ![]u8 {
        if (self.inMemMap.get(key)) |val| {
            return (try @constCast(&val).getValue(self));
        } else {
            return error.KeyNotFound;
        }
    }

    pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
        const timestamp = std.time.timestamp();
        const log = Log{
            .tstamp = timestamp,
            .keysz = key.len,
            .valuesz = value.len,
            .key = key,
            .value = value,
        };
        const offset = try self.file.write((try log.writeSerialized(self.allocator)));
        try self.inMemMap.put(key, KeyEntry{
            .offset = self.offset,
            .totalSize = offset,
            .timestamp = timestamp,
        });
        self.offset += offset;
    }
    pub fn delete() void {}
    pub fn listKeys() void {}
    pub fn merge(self: *Self) !void {
        // this might be a very naive way of implementing merge, but we basically flush the current hashmap after deleting its old contents
        // TODO: incredible CPU usage is expected cause of the lousy implementation, should ideally
        // have a reduced memory footprint(in place merge maybe?)

        try self.file.seekTo(0);
        var newInMemMap = std.StringHashMap(KeyEntry).init(self.allocator);
        var hashMapIter = self.inMemMap.iterator();
        var newBufferArrList = try std.ArrayList(u8).initCapacity(self.allocator, (try self.file.stat()).size);
        var newBufOffset: usize = 0;
        const newBufWriter = newBufferArrList.writer();
        while (hashMapIter.next()) |entry| {
            const val = try entry.value_ptr.getLog(self);
            const serialized = try val.writeSerialized(self.allocator);
            try newBufWriter.writeAll(serialized);
            const keyEntry = try self.allocator.create(KeyEntry);
            keyEntry.* = KeyEntry{
                .timestamp = val.tstamp,
                .offset = newBufOffset,
                .totalSize = serialized.len,
            };
            try newInMemMap.put(
                entry.key_ptr.*,
                keyEntry.*,
            );
            newBufOffset += serialized.len;
        }
        self.file.close();
        const newFile = try std.fs.cwd().openFile(self.fileName, .{ .mode = .read_write });
        try newFile.writeAll(newBufferArrList.items);
        self.inMemMap.deinit();
        self.inMemMap = newInMemMap;
        self.file = newFile;
    }
    pub fn sync() void {}
    pub fn close(self: *Self) !void {
        self.file.close();
    }
};

pub fn walkDirectory(allocator: std.mem.Allocator, path: []const u8) !std.BufSet {
    var dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
    defer dir.close();

    var file_list = std.BufSet.init(allocator);
    errdefer {
        file_list.deinit();
    }

    var walker = try dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next()) |entry| {
        if (entry.kind == .file) {
            try file_list.insert(entry.path);
        }
    }

    return file_list;
}

test "bitcask" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    _ = try BitcaskHandle.open("tst.db", .RW, allocator);
    // check if the tst.db file is created in root directory
    const list = try walkDirectory(allocator, ".");
    std.debug.assert(list.contains("tst.db"));
}

test "serializing a log" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const timestamp = std.time.timestamp();
    const handle = try BitcaskHandle.open("tst.db", .RW, allocator);
    const key = @as([]const u8, @constCast("key"));
    const value = @as([]const u8, @constCast("value"));
    const log = Log{
        .tstamp = timestamp,
        .keysz = key.len,
        .valuesz = value.len,
        .key = key,
        .value = value,
    };
    try handle.put(key, value);
    const a = try handle.get(key);
    std.log.warn("{any}\n", .{(try log.writeSerialized(allocator))});
    std.log.warn("read value: {s}\n", .{a});
}
test "open existing" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const handle = try BitcaskHandle.open("tst.db", .RW, allocator);
    const keys = [_][]const u8{
        "key1",
        "key2",
        "key3",
        "key4",
        "key1",
    };
    const values = [_][]const u8{
        "value1",
        "value2",
        "value3",
        "value4",
        "value5",
    };
    for (keys, values) |key, value| {
        try handle.put(key, value);
    }
    try handle.close();
    const openHandle = try BitcaskHandle.openExisting("tst.db", allocator);
    for (keys) |key| {
        std.log.warn("read value after open existing: {s}\n", .{(try openHandle.get(key))});
    }
    try openHandle.close();
}

test "merge" {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const handle = try BitcaskHandle.open("tst.db", .RW, allocator);
    const keys = [_][]const u8{
        "key1",
        "key2",
        "key3",
        "key4",
        "key1",
    };
    const values = [_][]const u8{
        "value1",
        "value2",
        "value3",
        "value4",
        "value5",
    };
    for (keys, values) |key, value| {
        try handle.put(key, value);
    }
    try handle.close();
    const openHandle = try BitcaskHandle.openExisting("tst.db", allocator);
    for (keys) |key| {
        std.log.warn("read value after open existing: {s}\n", .{(try openHandle.get(key))});
    }
    std.log.warn("size of file before merge: {} and hashmap: \n", .{(try openHandle.file.stat()).size});
    var hashMapIter = openHandle.inMemMap.iterator();
    while (hashMapIter.next()) |entry| {
        std.log.warn("{s} and {s}\n", .{ entry.key_ptr.*, (try entry.value_ptr.getValue(openHandle)) });
    }
    try openHandle.merge();
    hashMapIter = openHandle.inMemMap.iterator();
    std.log.warn("merged and new size = {} and hashmap: \n", .{(try openHandle.file.stat()).size});
    while (hashMapIter.next()) |entry| {
        std.log.warn("{s} and {s}\n", .{ entry.key_ptr.*, (try entry.value_ptr.getValue(openHandle)) });
    }
    try openHandle.close();
}
