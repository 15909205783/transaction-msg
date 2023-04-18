package com.example.transactionmsg.hook;

import java.util.Set;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public interface SendTXMsgHook {
  void afterSendSuccess(Message var1, SendResult var2) throws Exception;

  default String interestedTopic() {
    return null;
  }

  default Set<String> interestedTag() {
    return null;
  }
}
