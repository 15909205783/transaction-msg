package com.example.transactionmsg.constant;

import java.io.IOException;

public class ClientInitException extends IOException {
  public ClientInitException(String message) {
    super(message);
  }

  public ClientInitException(String message, Throwable cause) {
    super(message, cause);
  }
}
