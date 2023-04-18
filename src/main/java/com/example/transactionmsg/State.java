package com.example.transactionmsg;

public enum State {
  CREATE,
  RUNNING,
  CLOSED,
  FAILED;

  private State() {
  }
}
