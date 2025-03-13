package com.github.sbrachman.fat;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

class DirectoryManager {

    static final byte DELETED_ENTRY_MARKER = (byte) 0xE5;

    private final MappedByteBuffer directoryBuffer;
    private final DirectoryEntry[] entries;
    private final Map<String, Integer> nameToIndex = new HashMap<>();

    DirectoryManager(FileChannel fileChannel, int maxFiles, long directoryOffset) throws IOException {
        long directorySize = (long) maxFiles * DirectoryEntry.DIRECTORY_ENTRY_SIZE;
        this.directoryBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, directoryOffset, directorySize);
        this.directoryBuffer.order(LITTLE_ENDIAN);
        this.entries = new DirectoryEntry[maxFiles];
        load();
    }

    private void load() {
        for (int i = 0; i < entries.length; i++) {
            int pos = i * DirectoryEntry.DIRECTORY_ENTRY_SIZE;
            if (directoryBuffer.get(pos) == 0 || directoryBuffer.get(pos) == DELETED_ENTRY_MARKER) {
                continue;
            }
            directoryBuffer.position(pos);
            DirectoryEntry entry = DirectoryEntry.readFromBuffer(directoryBuffer);
            entries[i] = entry;
            nameToIndex.put(entry.fileName(), i);
        }
    }

    void updateEntry(int index, String fileName, int startBlock, int fileSize) {
        int pos = index * DirectoryEntry.DIRECTORY_ENTRY_SIZE;
        DirectoryEntry entry = new DirectoryEntry(fileName, startBlock, fileSize);
        directoryBuffer.position(pos);
        directoryBuffer.put(entry.toBuffer());
        entries[index] = entry;
        nameToIndex.put(fileName, index);
    }

    Optional<Integer> findFreeEntry() {
        for (int i = 0; i < entries.length; i++) {
            if (entries[i] == null) {
                return Optional.of(i);
            }
        }
        return Optional.empty();
    }

    Optional<DirectoryEntry> getEntry(String fileName) {
        return Optional.ofNullable(nameToIndex.get(fileName))
                .map(i -> entries[i]);
    }

    void markEntryDeleted(String fileName) {
        Integer index = nameToIndex.get(fileName);
        if (index == null) return;
        int pos = index * DirectoryEntry.DIRECTORY_ENTRY_SIZE;
        directoryBuffer.put(pos, DELETED_ENTRY_MARKER);
        entries[index] = null;
        nameToIndex.remove(fileName);
    }

    void updateFileSize(String fileName, int newSize) {
        Integer index = nameToIndex.get(fileName);
        if (index == null) throw new IllegalArgumentException("File not found: " + fileName);
        int pos = index * DirectoryEntry.DIRECTORY_ENTRY_SIZE + DirectoryEntry.FILENAME_MAX_LENGTH + 4;
        directoryBuffer.putInt(pos, newSize);
        entries[index] = new DirectoryEntry(fileName, entries[index].startBlock(), newSize);
    }

    Integer getEntryIndex(String fileName) {
        return nameToIndex.get(fileName);
    }

    void flush() {
        if (directoryBuffer != null) {
            directoryBuffer.force();
        }
    }
}