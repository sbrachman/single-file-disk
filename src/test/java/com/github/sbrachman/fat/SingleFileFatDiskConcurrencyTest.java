package com.github.sbrachman.fat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class SingleFileFatDiskConcurrencyTest {

    private static final int BLOCK_SIZE = 1024;
    private static final int MAX_DISK_SIZE = 1024 * 1024 * 64;
    private static final int MAX_NUM_OF_FILES = 1024;
    private static final int THREAD_COUNT = 16;

    private SingleFileFatDisk disk;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        File diskFile = tempDir.resolve("testdisk.img").toFile();
        diskFile.createNewFile();
        disk = SingleFileFatDisk.create(diskFile.getPath(), BLOCK_SIZE, MAX_DISK_SIZE, MAX_NUM_OF_FILES);
    }

    @Test
    void concurrent_reads() throws Exception {
        // given
        ByteBuffer data = ByteBuffer.wrap("test".getBytes());
        disk.createFile("file1", data);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Runnable task = () -> assertThatCode(() -> {
            ByteBuffer read = disk.readFile("file1");
            assertThat(new String(readAll(read))).isEqualTo("test");
        }).doesNotThrowAnyException();

        // when
        IntStream.range(0, 10).forEach(i -> executor.submit(task));
        executor.shutdown();

        // then
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void concurrent_writes() throws Exception {
        // given
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        IntStream.range(0, THREAD_COUNT).forEach(i -> executor.submit(() -> {
            assertThatCode(() -> {
                ByteBuffer data = ByteBuffer.wrap(("data_%d".formatted(i)).getBytes());
                disk.createFile("file_%d".formatted(i), data);
                latch.countDown();
            }).doesNotThrowAnyException();
        }));

        // when
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        // then
        IntStream.range(0, THREAD_COUNT).forEach(i -> assertThatCode(() -> {
            ByteBuffer read = disk.readFile("file_%d".formatted(i));
            assertThat(new String(readAll(read))).isEqualTo("data_%d".formatted(i));
        }).doesNotThrowAnyException());
    }

    @Test
    void concurrent_create_update_delete_read_with_tracking() throws Exception {
        // given:
        int fileCount = 1000;
        ConcurrentHashMap<String, String> expectedStates = new ConcurrentHashMap<>();

        for (int i = 0; i < fileCount; i++) {
            String fileName = "file_%d".formatted(i);
            String initialContent = "initial_%d".formatted(i);
            disk.createFile(fileName, ByteBuffer.wrap(initialContent.getBytes()));
            expectedStates.put(fileName, initialContent);
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        // when:
        for (int i = 0; i < fileCount; i++) {
            int fileId = i;
            executor.submit(() -> {
                try {
                    String fileName = "file_%d".formatted(fileId);
                    int operation = ThreadLocalRandom.current().nextInt(4);

                    switch (operation) {
                        case 0 -> {
                            // delete
                            disk.deleteFile(fileName);
                            expectedStates.remove(fileName);
                        }
                        case 1 -> {
                            // replace
                            String newContent = "updated_%d".formatted(fileId);
                            disk.createFile(fileName, ByteBuffer.wrap(newContent.getBytes()));
                            expectedStates.put(fileName, newContent);
                        }
                        case 2 -> {
                            // read
                            String expectedContent = expectedStates.get(fileName);
                            ByteBuffer content = disk.readFile(fileName);
                            assertThat(new String(readAll(content))).isEqualTo(expectedContent);
                        }
                        case 3 -> {
                            // append
                            ByteBuffer content = disk.readFile(fileName);
                            String appended = "a".repeat(BLOCK_SIZE * 2);
                            disk.appendFile(fileName, ByteBuffer.wrap(appended.getBytes()));
                            byte[] updatedData = combine(readAll(content), appended.getBytes());
                            assertThat(readAll(disk.readFile(fileName))).isEqualTo(updatedData);
                            expectedStates.put(fileName, new String(updatedData));
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        // then:
        expectedStates.forEach((fileName, expectedContent) ->
                assertThatCode(() -> {
                    ByteBuffer content = disk.readFile(fileName);
                    assertThat(new String(readAll(content))).isEqualTo(expectedContent);
                }).doesNotThrowAnyException()
        );

        IntStream.range(0, fileCount)
                .mapToObj("file_%d"::formatted)
                .filter(fileName -> !expectedStates.containsKey(fileName))
                .forEach(fileName ->
                        assertThatExceptionOfType(IOException.class)
                                .isThrownBy(() -> disk.readFile(fileName))
                );
    }

    public byte[] readAll(ByteBuffer buffer) {
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    public byte[] combine(byte[] bytes1, byte[] bytes2) {
        byte[] combined = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, combined, 0, bytes1.length);
        System.arraycopy(bytes2, 0, combined, bytes1.length, bytes2.length);
        return combined;
    }
}