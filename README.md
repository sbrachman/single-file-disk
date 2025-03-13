# Single File Disk: A File Allocation Table (FAT) Based Virtual Disk

## Project Description

A Java implementation of a FAT-style virtual disk system supporting:

- **File Operations**: Create/read/append/delete files
- **Concurrency**: Thread-safe operations using `ReentrantReadWriteLock`
- **Persistence**: Disk-like storage in a single file
- **Allocation**: 4KB blocks (configurable) with FAT chaining
- **Limits**:
    - 24-byte filenames
    - 65,536 max files (configurable)
    - 1GB max disk size (configurable)

## Storage Layout Diagram

| Header (16B)    | FAT         | Directory   | Data Blocks |
|-----------------|-------------|-------------|-------------|
| blockSize (4B)  | FAT entries | Filename    | Block 0     |
| fatEntries (4B) | (262k×4B)   | Start Block | (0-4KB)     |
| maxFiles (4B)   |             | File Size   | Block 1     |
| reserved (4B)   |             | ...         | (4-8KB)     |

### 1. Format Header Section (16 bytes)

| Block Size (4B) | Number of FAT Entries (4B) | Max Number of Files (4B) | Reserved (4B) |
|-----------------|----------------------------|--------------------------|---------------|

### 2. FAT (File Allocation Table)

- **262,144 entries** × 4 bytes (1GB/4KB blocks)
- **Chain Example**:

  | Index | Next Block |
  |-------|------------|
  | 0     | 1          |
  | 1     | -1 (EOF)   |
  | 2     | 3          |
  | 3     | 4          |
  | 4     | -1 (EOF)   |

### 3. Directory Entries

- **65,536 entries** × 32 bytes each:

| Filename (24B)      | Start Block | File Size |
|---------------------|-------------|-----------|
| report.txt          | 0           | 8192      |
| [0xE5]... (deleted) | —           | —         |
| image.jpg           | 2           | 12288     |

### 4. Data Blocks

- **262,144 × 4KB blocks**:

| Block   | Status | Size Range |
|---------|--------|------------|
| Block 0 | USED   | 0-4KB      |
| Block 1 | FREE   | 4-8KB      |
| Block 2 | USED   | 8-12KB     |

## Usage Example

```java
// Create disk & write file
try (SingleFileDisk disk = SingleFileFatDisk.create("mydisk.fat")) {
    ByteBuffer data = ByteBuffer.wrap("Hello World".getBytes());
    disk.createFile("test.txt", data);

    // Read back
    ByteBuffer content = disk.readFile("test.txt");
    System.out.println(UTF_8.decode(content));
}
```

## Build Project

```shell
./gradlew clean build 
```
