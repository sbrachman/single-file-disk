package com.github.sbrachman.fat;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.Optional;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

class FatManager {

    static final int END_OF_CHAIN = -1;

    private final MappedByteBuffer fatBuffer;
    private final BitSet freeBlocks;

    FatManager(FileChannel fileChannel, int fatEntriesCount, long fatOffset) throws IOException {
        this.fatBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, fatOffset, (long) fatEntriesCount * 4);
        this.fatBuffer.order(LITTLE_ENDIAN);
        this.freeBlocks = new BitSet(fatEntriesCount);
        load();
    }

    private void load() {
        for (int i = 0; i < freeBlocks.size(); i++) {
            if (fatBuffer.getInt(fatIndex(i)) == 0) {
                freeBlocks.set(i);
            }
        }
    }

    Optional<int[]> allocateBlocks(int blocksNeeded) {
        int[] blocks = new int[blocksNeeded];
        int fromIndex = 0;
        for (int i = 0; i < blocksNeeded; i++) {
            int next = freeBlocks.nextSetBit(fromIndex);
            if (next == -1) {
                return Optional.empty();
            }
            blocks[i] = next;
            freeBlocks.clear(next);
            fromIndex = next + 1;
        }
        return Optional.of(blocks);
    }

    void updateFatChain(int[] blocks) {
        for (int i = 0; i < blocks.length; i++) {
            int next = (i == blocks.length - 1) ? END_OF_CHAIN : blocks[i + 1];
            fatBuffer.putInt(fatIndex(blocks[i]), next);
        }
    }

    void freeBlocks(int startBlock) {
        int current = startBlock;
        while (current >= 0 && current < freeBlocks.size()) {
            int next = fatBuffer.getInt(fatIndex(current));
            fatBuffer.putInt(fatIndex(current), 0);
            freeBlocks.set(current);
            current = next;
        }
    }

    int nextBlock(int current) {
        return fatBuffer.getInt(fatIndex(current));
    }

    void updateFatEntry(int block, int nextBlock) {
        fatBuffer.putInt(fatIndex(block), nextBlock);
    }

    void flush() {
        if (fatBuffer != null) {
            fatBuffer.force();
        }
    }

    private int fatIndex(int blockNumber) {
        return blockNumber * 4;
    }
}