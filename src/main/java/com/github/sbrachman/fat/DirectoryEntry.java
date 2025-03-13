package com.github.sbrachman.fat;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;

record DirectoryEntry(String fileName, int startBlock, int fileSize) {

    static final int FILENAME_MAX_LENGTH = 24;
    static final int DIRECTORY_ENTRY_SIZE = FILENAME_MAX_LENGTH + 4 + 4;

    ByteBuffer toBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(DIRECTORY_ENTRY_SIZE).order(LITTLE_ENDIAN);
        buffer.put(Arrays.copyOf(fileName.getBytes(UTF_8), FILENAME_MAX_LENGTH));
        buffer.putInt(startBlock);
        buffer.putInt(fileSize);
        buffer.flip();
        return buffer;
    }

    static DirectoryEntry readFromBuffer(ByteBuffer buffer) {
        byte[] nameBytes = new byte[FILENAME_MAX_LENGTH];
        buffer.get(nameBytes);
        String fileName = new String(nameBytes, UTF_8).trim();
        int startBlock = buffer.getInt();
        int fileSize = buffer.getInt();
        return new DirectoryEntry(fileName, startBlock, fileSize);
    }
}
