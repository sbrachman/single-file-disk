package com.github.sbrachman.fat;

import com.github.sbrachman.SingleFileDisk;
import com.github.sbrachman.exception.DirectoryFullException;
import com.github.sbrachman.exception.InsufficientSpaceException;
import com.github.sbrachman.exception.InvalidFileNameException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class SingleFileFatDisk implements SingleFileDisk {

    private final FileChannel fileChannel;
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final FatManager fatManager;
    private final DirectoryManager directoryManager;
    private final BlockStorage blockStorage;
    private final FormatHeader formatHeader;

    public static SingleFileFatDisk create(String filename, int blockSizeBytes, int maxDiskSizeBytes, int maxNumOfFiles) throws IOException {
        int fatEntries = maxDiskSizeBytes / blockSizeBytes;
        return new SingleFileFatDisk(filename, new FormatHeader(blockSizeBytes, fatEntries, maxNumOfFiles), true);
    }

    public static SingleFileFatDisk create(String filename) throws IOException {
        return create(filename, FormatHeader.DEFAULT_BLOCK_SIZE, FormatHeader.DEFAULT_MAX_DISK_SIZE_BYTES, FormatHeader.DEFAULT_MAX_NUM_OF_FILES);
    }

    public static SingleFileFatDisk loadFromFile(String filename) throws IOException {
        Path diskPath = Paths.get(filename);
        if (!Files.exists(diskPath)) {
            throw new FileNotFoundException("Disk file not found: %s".formatted(filename));
        }

        try (FileChannel channel = FileChannel.open(diskPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer headerBuffer = ByteBuffer.allocate(FormatHeader.HEADER_SIZE).order(LITTLE_ENDIAN);
            channel.read(headerBuffer);
            headerBuffer.flip();

            int loadedBlockSize = headerBuffer.getInt();
            int loadedFatEntries = headerBuffer.getInt();
            int loadedMaxFiles = headerBuffer.getInt();
            FormatHeader header = new FormatHeader(loadedBlockSize, loadedFatEntries, loadedMaxFiles);

            return new SingleFileFatDisk(filename, header, false);
        }
    }

    @Override
    public void createFile(String fileName) throws DirectoryFullException, InvalidFileNameException {
        rwLock.writeLock().lock();
        try {
            validateFileName(fileName);
            deleteIfPresent(fileName);

            Integer entryIndex = directoryManager.findFreeEntry()
                    .orElseThrow(() -> new DirectoryFullException("Root directory full"));

            directoryManager.updateEntry(entryIndex, fileName, FatManager.END_OF_CHAIN, 0);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void createFile(String fileName, ByteBuffer data) throws IOException {
        rwLock.writeLock().lock();
        try {
            validateFileName(fileName);
            deleteIfPresent(fileName);

            Integer entryIndex = directoryManager.findFreeEntry()
                    .orElseThrow(() -> new DirectoryFullException("Root directory full"));

            int blocksNeeded = (data.remaining() + formatHeader.blockSize() - 1) / formatHeader.blockSize();
            int[] allocatedBlocks = fatManager.allocateBlocks(blocksNeeded)
                    .orElseThrow(() -> new InsufficientSpaceException("Not enough free space"));

            blockStorage.write(allocatedBlocks, data.duplicate());
            fatManager.updateFatChain(allocatedBlocks);
            directoryManager.updateEntry(entryIndex, fileName, allocatedBlocks[0], data.remaining());
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void appendFile(String fileName, ByteBuffer data) throws IOException {
        rwLock.writeLock().lock();
        try {
            validateFileName(fileName);
            DirectoryEntry entry = directoryManager.getEntry(fileName)
                    .orElseThrow(() -> new FileNotFoundException("File not found: %s".formatted(fileName)));

            int currentFileSize = entry.fileSize();
            int startBlock = entry.startBlock();

            if (startBlock == FatManager.END_OF_CHAIN) {
                int[] newBlocks = fatManager.allocateBlocks(1)
                        .orElseThrow(() -> new InsufficientSpaceException("Not enough space to append"));
                startBlock = newBlocks[0];
                fatManager.updateFatChain(newBlocks);
                directoryManager.updateEntry(directoryManager.getEntryIndex(fileName), fileName, startBlock, 0);
            }

            int lastBlock = startBlock;
            int nextBlock = fatManager.nextBlock(lastBlock);
            while (nextBlock != FatManager.END_OF_CHAIN) {
                lastBlock = nextBlock;
                nextBlock = fatManager.nextBlock(lastBlock);
            }

            int offset = currentFileSize % formatHeader.blockSize();
            int spaceLeft = formatHeader.blockSize() - offset;
            int bytesToAppend = data.remaining();
            int bytesWritten = 0;

            if (spaceLeft > 0) {
                int writeBytes = Math.min(spaceLeft, bytesToAppend);
                ByteBuffer slice = data.slice();
                slice.limit(writeBytes);
                blockStorage.appendToBlock(lastBlock, offset, slice);
                data.position(data.position() + writeBytes);
                bytesWritten += writeBytes;
            }

            int remaining = bytesToAppend - bytesWritten;
            if (remaining > 0) {
                int blocksNeeded = (remaining + formatHeader.blockSize() - 1) / formatHeader.blockSize();
                int[] newBlocks = fatManager.allocateBlocks(blocksNeeded)
                        .orElseThrow(() -> new InsufficientSpaceException("Not enough space to append"));

                fatManager.updateFatEntry(lastBlock, newBlocks[0]);
                fatManager.updateFatChain(newBlocks);
                blockStorage.write(newBlocks, data);
            }

            directoryManager.updateFileSize(fileName, currentFileSize + bytesToAppend);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public ByteBuffer readFile(String fileName) throws IOException {
        rwLock.readLock().lock();
        try {
            DirectoryEntry entry = directoryManager.getEntry(fileName)
                    .orElseThrow(() -> new FileNotFoundException("File not found: %s".formatted(fileName)));

            ByteBuffer fileData = ByteBuffer.allocateDirect(entry.fileSize());
            int currentBlock = entry.startBlock();
            int bytesRead = 0;

            while (currentBlock != FatManager.END_OF_CHAIN && bytesRead < entry.fileSize()) {
                ByteBuffer blockBuffer = blockStorage.readBlock(currentBlock);
                int bytesToRead = Math.min(blockBuffer.remaining(), entry.fileSize() - bytesRead);
                blockBuffer.limit(blockBuffer.position() + bytesToRead);
                fileData.put(blockBuffer);
                bytesRead += bytesToRead;
                currentBlock = fatManager.nextBlock(currentBlock);
            }
            fileData.flip();
            return fileData;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void deleteFile(String fileName) throws FileNotFoundException {
        if (!deleteIfPresent(fileName)) {
            throw new FileNotFoundException("File not found: %s".formatted(fileName));
        }
    }

    @Override
    public void close() throws IOException {
        rwLock.writeLock().lock();
        try {
            fatManager.flush();
            directoryManager.flush();
            fileChannel.force(true);
            fileChannel.close();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private SingleFileFatDisk(String filename, FormatHeader header, boolean createNewDisk) throws IOException {
        this.formatHeader = header;
        Path diskPath = Paths.get(filename);

        if (createNewDisk) {
            if (Files.exists(diskPath)) Files.delete(diskPath);
            fileChannel = FileChannel.open(diskPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            initializeDisk(header);
        } else {
            if (!Files.exists(diskPath)) throw new FileNotFoundException("Disk file not found: %s".formatted(filename));
            fileChannel = FileChannel.open(diskPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        long fatOffset = FormatHeader.HEADER_SIZE;
        long directoryOffset = fatOffset + (long) header.fatEntries() * 4;
        long dataOffset = directoryOffset + (long) header.maxFiles() * DirectoryEntry.DIRECTORY_ENTRY_SIZE;

        fatManager = new FatManager(fileChannel, header.fatEntries(), fatOffset);
        directoryManager = new DirectoryManager(fileChannel, header.maxFiles(), directoryOffset);
        blockStorage = new BlockStorage(fileChannel, dataOffset, header.blockSize());
    }

    private void validateFileName(String fileName) throws InvalidFileNameException {
        if (isBlank(fileName)) {
            throw new InvalidFileNameException("Filename cannot be empty");
        }
        if (fileName.getBytes(UTF_8).length > DirectoryEntry.FILENAME_MAX_LENGTH) {
            throw new InvalidFileNameException(
                    "Filename '%s' exceeds %d bytes".formatted(fileName, DirectoryEntry.FILENAME_MAX_LENGTH)
            );
        }
    }

    private boolean deleteIfPresent(String fileName) {
        rwLock.writeLock().lock();
        try {
            return directoryManager.getEntry(fileName)
                    .map(entry -> {
                        fatManager.freeBlocks(entry.startBlock());
                        directoryManager.markEntryDeleted(fileName);
                        return true;
                    }).orElse(false);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void initializeDisk(FormatHeader header) throws IOException {
        initializeHeader(header);
        initializeFat(header.fatEntries());
    }

    private void initializeFat(int fatEntriesCount) throws IOException {
        ByteBuffer fatBuffer = ByteBuffer.allocate(fatEntriesCount * 4).order(LITTLE_ENDIAN);
        fileChannel.write(fatBuffer, FormatHeader.HEADER_SIZE);
    }

    private void initializeHeader(FormatHeader header) throws IOException {
        fileChannel.write(header.serialize());
    }
}