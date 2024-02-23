package com.example.transactionmsg.common;

import org.apache.pulsar.client.api.MessageId;


public class SendResult {

  private MessageId messageId;

  public SendResult(MessageId messageId) {
    this.messageId = messageId;
  }

  public MessageId getMessageId() {
    return messageId;
  }

  public void setMessageId(MessageId messageId) {
    this.messageId = messageId;
  }
}
