package com.github.sbrachman.fat;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

record FormatHeader(int blockSize, int fatEntries, int maxFiles) {
    public static final int HEADER_SIZE = 16;
    public static final int DEFAULT_BLOCK_SIZE = 4096;
    public static final int DEFAULT_MAX_DISK_SIZE_BYTES = 1024 * 1024 * 1024;
    public static final int DEFAULT_MAX_NUM_OF_FILES = 64 * 1024;

    ByteBuffer serialize() {
        return ByteBuffer.allocate(HEADER_SIZE)
                .order(LITTLE_ENDIAN)
                .putInt(blockSize)
                .putInt(fatEntries)
                .putInt(maxFiles)
                .putInt(0) // 4 bytes reserved
                .flip();
    }
}