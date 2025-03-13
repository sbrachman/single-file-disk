package com.github.sbrachman.exception;

import java.io.IOException;

public class InvalidBlockOperationException extends IOException {

    public InvalidBlockOperationException(int block, int offset, int blockSize) {
        super("Invalid operation on block %d: Offset %d exceeds block size %d".formatted(block, offset, blockSize));
    }
}