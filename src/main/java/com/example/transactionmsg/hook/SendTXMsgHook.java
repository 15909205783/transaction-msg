package com.example.transactionmsg.hook;

import com.example.transactionmsg.common.SendResult;
import com.example.transactionmsg.common.message.Message;
import java.util.Set;

public interface SendTXMsgHook {
  void afterSendSuccess(Message message, SendResult sendResult) throws Exception;

  default String interestedTopic() {
    return null;
  }

  default Set<String> interestedTag() {
    return null;
  }
}
