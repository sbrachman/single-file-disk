package com.github.sbrachman;

import com.github.sbrachman.exception.DirectoryFullException;
import com.github.sbrachman.exception.InvalidFileNameException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface SingleFileDisk extends AutoCloseable {

    void createFile(String fileName) throws DirectoryFullException, InvalidFileNameException;

    void createFile(String fileName, ByteBuffer data) throws IOException;

    void appendFile(String fileName, ByteBuffer data) throws IOException;

    ByteBuffer readFile(String fileName) throws IOException;

    void deleteFile(String fileName) throws FileNotFoundException;

    @Override
    void close() throws IOException;
}
