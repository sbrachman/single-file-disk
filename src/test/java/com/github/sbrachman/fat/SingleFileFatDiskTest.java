package com.github.sbrachman.fat;

import com.github.sbrachman.SingleFileDisk;
import com.github.sbrachman.exception.InvalidFileNameException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SingleFileFatDiskTest {

    private static final int BLOCK_SIZE = 4096; //4KB
    private static final int MAX_DISK_SIZE = 1024 * 1024 * 64; //64MB
    private static final int MAX_NUM_OF_FILES = 1024;
    private static final int FILENAME_MAX_LENGTH = 24;

    private File diskFile;
    private SingleFileDisk disk;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        diskFile = tempDir.resolve("testdisk.img").toFile();
        diskFile.createNewFile();
        disk = SingleFileFatDisk.create(diskFile.getPath(), BLOCK_SIZE, MAX_DISK_SIZE, MAX_NUM_OF_FILES);
    }

    @Test
    void create_and_read_small_file() throws IOException {
        // given
        byte[] data = "Test content".getBytes();

        // when
        disk.createFile("test.txt", ByteBuffer.wrap(data));

        // then
        assertThat(readAll(disk.readFile("test.txt"))).isEqualTo(data);
    }

    @Test
    void create_duplicate_file_overwrites_content() throws IOException {
        // given
        byte[] initialData = "Old content".getBytes();
        byte[] newData = "New content".getBytes();
        disk.createFile("file.txt", ByteBuffer.wrap(initialData));

        // when
        disk.createFile("file.txt", ByteBuffer.wrap(newData));

        // then
        assertThat(readAll(disk.readFile("file.txt"))).isEqualTo(newData);
    }

    @Test
    void read_nonexistent_file_throws_exception() {
        // when
        // then
        assertThatThrownBy(() -> readAll(disk.readFile("nonexistent.txt")))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void delete_existing_file_removes_entry() throws IOException {
        // given
        disk.createFile("to_delete.txt", ByteBuffer.allocate(10)); // Use allocate

        // when
        disk.deleteFile("to_delete.txt");

        // then
        assertThatThrownBy(() -> disk.readFile("to_delete.txt"))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void create_and_delete_files_in_loop() throws IOException {
        for (int i = 0; i < 50; i++) {
            // given
            String filename = "temp_file_" + i + ".txt";
            byte[] data = ("Temporary content " + i).getBytes();
            disk.createFile(filename, ByteBuffer.wrap(data));
            assertThat(readAll(disk.readFile(filename))).isEqualTo(data);

            // when
            disk.deleteFile(filename);

            // then
            assertThatThrownBy(() -> disk.readFile(filename)).isInstanceOf(FileNotFoundException.class);
        }
    }

    @Test
    void should_load_existing_disk() throws IOException {
        // given
        for (int i = 0; i < 50; i++) {
            byte[] data = ("Temporary content %d".formatted(i)).getBytes();
            String filename = "temp_file_%d.txt".formatted(i);
            disk.createFile(filename, ByteBuffer.wrap(data));
        }
        disk.close();

        // when
        try (SingleFileFatDisk loadedFromExistingFile = SingleFileFatDisk.loadFromFile(diskFile.getPath())) {
            // then
            for (int i = 0; i < 50; i++) {
                String filename = "temp_file_%d.txt".formatted(i);
                byte[] expectedData = ("Temporary content %d".formatted(i)).getBytes();
                byte[] existingData = readAll(loadedFromExistingFile.readFile(filename));
                assertThat(existingData).isEqualTo(expectedData);
            }
        }
    }

    @Test
    void should_create_files_with_different_sizes() throws IOException {
        // given
        int smallFileSize = BLOCK_SIZE / 2;
        int mediumFileSize = BLOCK_SIZE * 2;
        int largeFileSize = BLOCK_SIZE * 50;
        byte[] smallFile = randomData(smallFileSize);
        byte[] mediumFile = randomData(mediumFileSize);
        byte[] largeFile = randomData(largeFileSize);

        // when
        disk.createFile("small_file.txt", ByteBuffer.wrap(smallFile));
        disk.createFile("medium_file.txt", ByteBuffer.wrap(mediumFile));
        disk.createFile("large_file.txt", ByteBuffer.wrap(largeFile));

        // then
        assertThat(readAll(disk.readFile("small_file.txt"))).isEqualTo(smallFile);
        assertThat(readAll(disk.readFile("medium_file.txt"))).isEqualTo(mediumFile);
        assertThat(readAll(disk.readFile("large_file.txt"))).isEqualTo(largeFile);
    }

    @Test
    void create_max_files_delete_recreate_verify() throws IOException {
        // given
        Map<String, byte[]> createdFiles = new HashMap<>();

        for (int i = 0; i < MAX_NUM_OF_FILES; i++) {
            String filename = "file_%d.txt".formatted(i);
            byte[] data = ("Initial content for file %d".formatted(i)).getBytes();
            disk.createFile(filename, ByteBuffer.wrap(data));
            createdFiles.put(filename, data);
        }

        for (Map.Entry<String, byte[]> entry : createdFiles.entrySet()) {
            assertThat(readAll(disk.readFile(entry.getKey()))).isEqualTo(entry.getValue());
        }

        // when
        for (String filename : createdFiles.keySet()) {
            disk.deleteFile(filename);
            assertThatThrownBy(() -> disk.readFile(filename)).isInstanceOf(FileNotFoundException.class);
        }
        createdFiles.clear();

        for (int i = 0; i < MAX_NUM_OF_FILES; i++) {
            String filename = "file_%d.txt".formatted(i);
            byte[] newData = randomData(BLOCK_SIZE);
            disk.createFile(filename, ByteBuffer.wrap(newData));
            createdFiles.put(filename, newData);
        }

        // then
        for (Map.Entry<String, byte[]> entry : createdFiles.entrySet()) {
            assertThat(readAll(disk.readFile(entry.getKey()))).isEqualTo(entry.getValue());
        }
    }

    @Test
    void create_file_exceeding_disk_space_throws_exception() {
        // given
        ByteBuffer hugeData = ByteBuffer.allocate(MAX_DISK_SIZE + 1); // Use allocate

        // when
        // then
        assertThatThrownBy(() -> disk.createFile("huge.bin", hugeData))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Not enough free space");
    }

    @Test
    void create_file_when_directory_full_throws_exception() throws IOException {
        // given
        for (int i = 0; i < MAX_NUM_OF_FILES; i++) {
            disk.createFile("file_%d.txt".formatted(i), ByteBuffer.wrap("data_%d.txt".formatted(i).getBytes()));
        }

        // when
        // then
        assertThatThrownBy(() -> disk.createFile("extra.txt", ByteBuffer.wrap("extra_data".getBytes())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Root directory full");
    }

    @Test
    void deleted_blocks_are_reused() throws IOException {
        // given
        byte[] bigData = randomData(MAX_DISK_SIZE); // Use allocate
        disk.createFile("big.bin", ByteBuffer.wrap(bigData)); // Fill and flip
        disk.deleteFile("big.bin");

        // when
        byte[] updatedBigData = randomData(MAX_DISK_SIZE);
        disk.createFile("reused.bin", ByteBuffer.wrap(updatedBigData));

        // then
        assertThat(readAll(disk.readFile("reused.bin"))).hasSize(MAX_DISK_SIZE);
        assertThat(readAll(disk.readFile("reused.bin"))).isEqualTo(updatedBigData);
    }

    @Test
    void append_to_file() throws IOException {
        // given
        disk.createFile("append.txt", ByteBuffer.wrap("Initial content".getBytes()));

        // when
        disk.appendFile("append.txt", ByteBuffer.wrap(" Appended content".getBytes()));

        // then
        assertThat(asString(disk.readFile("append.txt"))).isEqualTo("Initial content Appended content");
    }

    @Test
    void append_multiple_times() throws IOException {
        // given
        disk.createFile("multi_append.txt", ByteBuffer.wrap("First part".getBytes()));

        // when
        disk.appendFile("multi_append.txt", ByteBuffer.wrap(" Second part".getBytes()));
        disk.appendFile("multi_append.txt", ByteBuffer.wrap(" Third part".getBytes()));

        // then
        assertThat(asString(disk.readFile("multi_append.txt"))).isEqualTo("First part Second part Third part");
    }

    @Test
    void append_file_larger_than_block_size() throws IOException {
        // given
        byte[] initialData = "Initial ".getBytes();
        byte[] appendData = randomData(BLOCK_SIZE * 2);
        disk.createFile("large_append.txt", ByteBuffer.wrap(initialData));

        // when
        disk.appendFile("large_append.txt", ByteBuffer.wrap(appendData));

        // then
        byte[] expectedData = new byte[initialData.length + appendData.length];
        System.arraycopy(initialData, 0, expectedData, 0, initialData.length);
        System.arraycopy(appendData, 0, expectedData, initialData.length, appendData.length);

        assertThat(readAll(disk.readFile("large_append.txt"))).isEqualTo(expectedData);
    }

    @Test
    void append_to_nonexistent_file_throws_exception() {
        assertThatThrownBy(() -> disk.appendFile("nonexistent_append.txt", ByteBuffer.wrap("content".getBytes())))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void create_empty_file() throws IOException {
        // when
        disk.createFile("empty.txt");

        // then
        assertThat(readAll(disk.readFile("empty.txt"))).isEmpty();
    }

    @Test
    void create_empty_file_and_append() throws IOException {
        // given
        disk.createFile("empty_then_append.txt");

        // when
        disk.appendFile("empty_then_append.txt", ByteBuffer.wrap("Appended content".getBytes()));

        // then
        assertThat(asString(disk.readFile("empty_then_append.txt"))).isEqualTo("Appended content");
    }

    @Test
    void filename_too_long_throws_exception() {
        // given
        String longName = "a".repeat(FILENAME_MAX_LENGTH + 1);

        // when
        // then
        assertThatThrownBy(() -> disk.createFile(longName, ByteBuffer.allocate(10))) // Use allocate
                .isInstanceOf(InvalidFileNameException.class)
                .hasMessageContaining("Filename '%s' exceeds %s bytes".formatted(longName, FILENAME_MAX_LENGTH));
    }

    @Test
    void create_file_with_max_filename_length() throws IOException {
        // given
        String maxName = "f".repeat(FILENAME_MAX_LENGTH);
        byte[] data = "Valid content".getBytes();

        // when
        disk.createFile(maxName, ByteBuffer.wrap(data));

        // then
        assertThat(readAll(disk.readFile(maxName))).isEqualTo(data);
    }

    @Test
    void filename_with_unicode_characters() throws IOException {
        // given
        String fileName = "测试文件.txt";
        byte[] data = "Unicode content".getBytes();

        // when
        disk.createFile(fileName, ByteBuffer.wrap(data));

        // then
        assertThat(readAll(disk.readFile(fileName))).isEqualTo(data);
    }

    private byte[] readAll(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    private String asString(ByteBuffer buffer) {
        return new String(readAll(buffer), UTF_8);
    }

    private static byte[] randomData(int size) {
        byte[] arr = new byte[size];
        new Random().nextBytes(arr);
        return arr;
    }
}