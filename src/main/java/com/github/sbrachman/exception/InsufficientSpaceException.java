package com.github.sbrachman.exception;

import java.io.IOException;

public class InsufficientSpaceException extends IOException {
  public InsufficientSpaceException(String message) {
    super(message);
  }
}