package com.github.sbrachman.exception;

import java.io.IOException;

public class InvalidFileNameException extends IOException {
  public InvalidFileNameException(String message) {
    super(message);
  }
}
