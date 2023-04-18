package com.example.transactionmsg.common;

public class TXMQDriver {
  private TXMQDriver() {
  }

  static {
    TXMQVersion.setCurrentVersionProp();
  }
}
