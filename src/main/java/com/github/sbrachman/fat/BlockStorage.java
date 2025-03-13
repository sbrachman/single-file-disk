package com.github.sbrachman.fat;

import com.github.sbrachman.exception.InvalidBlockOperationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class BlockStorage {

    private final FileChannel channel;
    private final long baseOffset;
    private final int blockSize;

    BlockStorage(FileChannel channel, long baseOffset, int blockSize) {
        this.channel = channel;
        this.baseOffset = baseOffset;
        this.blockSize = blockSize;
    }

    void write(int[] blocks, ByteBuffer data) throws IOException {
        for (int block : blocks) {
            int bytesToWrite = Math.min(blockSize, data.remaining());
            ByteBuffer slice = data.slice();
            slice.limit(bytesToWrite);
            channel.write(slice, baseOffset + (long) block * blockSize);
            data.position(data.position() + bytesToWrite);
        }
    }

    void appendToBlock(int block, int offset, ByteBuffer data) throws IOException {
        if (offset < 0 || offset >= blockSize) {
            throw new InvalidBlockOperationException(block, offset, blockSize);
        }
        long position = baseOffset + (long) block * blockSize + offset;
        int bytesToWrite = Math.min(blockSize - offset, data.remaining());
        ByteBuffer slice = data.slice();
        slice.limit(bytesToWrite);
        channel.write(slice, position);
        data.position(data.position() + bytesToWrite);
    }

    ByteBuffer readBlock(int block) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocateDirect(blockSize);
        channel.read(buffer, baseOffset + (long) block * blockSize);
        buffer.flip();
        return buffer;
    }
}