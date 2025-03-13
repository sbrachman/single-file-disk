package com.github.sbrachman.exception;

import java.io.IOException;

public class DirectoryFullException extends IOException {
  public DirectoryFullException(String message) {
    super(message);
  }

  public DirectoryFullException(String message, int maxEntries) {
    super("%s (Max entries: %d)".formatted(message, maxEntries));
  }
}