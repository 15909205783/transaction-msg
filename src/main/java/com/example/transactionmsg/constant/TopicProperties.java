package com.example.transactionmsg.constant;

import java.io.Serializable;

import org.apache.pulsar.client.api.CompressionType;

/**
 * @author fuhan
 * @date 2022/8/12 - 11:43
 */
public class TopicProperties implements Serializable {

  private static final long serialVersionUID = -7163802658597185501L;

  /**
   * topic名称
   */
  private String name;

  /**
   * topic 消息类型
   */
  private Class<?> msgClass = byte[].class;

  /**
   * 是否开启批量发送
   */
  private boolean batchingEnabled = true;

  /**
   * 是否是持久化topic
   */
  private boolean persistent = true;

  /**
   * 设置包含待处理消息的队列的最大大小，以便从broker接收确认。 当队列已满时，默认的，所有的调用都会失败，除非blockIfQueueFull设置为true。可以使用blockIfQueueFull来改变这个行为。
   */
  private int maxPendingMessages = 1000;

  /**
   * 当输出（outgoing）队列已满时，是否停止相应的操作
   */
  private boolean blockIfQueueFull = true;

  /**
   * 发送超时时间 (ms)
   */
  private int sendTimeoutMs = 30000;

  /**
   * Set the compression type for the producer.
   *
   * <p>By default, message payloads are not compressed. Supported compression types are:
   *
   * <ul>
   *   <li>{@link CompressionType#NONE}: No compression (Default)
   *   <li>{@link CompressionType#LZ4}: Compress with LZ4 algorithm. Faster but lower compression
   *       than ZLib
   *   <li>{@link CompressionType#ZLIB}: Standard ZLib compression
   *   <li>{@link CompressionType#ZSTD} Compress with Zstandard codec. Since Pulsar 2.3. Zstd cannot
   *       be used if consumer applications are not in version >= 2.3 as well
   *   <li>{@link CompressionType#SNAPPY} Compress with Snappy codec. Since Pulsar 2.4. Snappy
   *       cannot be used if consumer applications are not in version >= 2.4 as well
   * </ul>
   */
  private CompressionType compressionType = null;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Class<?> getMsgClass() {
    return msgClass;
  }

  public void setMsgClass(Class<?> msgClass) {
    this.msgClass = msgClass;
  }

  public boolean isBatchingEnabled() {
    return batchingEnabled;
  }

  public void setBatchingEnabled(boolean batchingEnabled) {
    this.batchingEnabled = batchingEnabled;
  }

  public boolean isPersistent() {
    return persistent;
  }

  public void setPersistent(boolean persistent) {
    this.persistent = persistent;
  }

  public int getMaxPendingMessages() {
    return maxPendingMessages;
  }

  public void setMaxPendingMessages(int maxPendingMessages) {
    this.maxPendingMessages = maxPendingMessages;
  }

  public boolean isBlockIfQueueFull() {
    return blockIfQueueFull;
  }

  public void setBlockIfQueueFull(boolean blockIfQueueFull) {
    this.blockIfQueueFull = blockIfQueueFull;
  }

  public int getSendTimeoutMs() {
    return sendTimeoutMs;
  }

  public void setSendTimeoutMs(int sendTimeoutMs) {
    this.sendTimeoutMs = sendTimeoutMs;
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
  }
}
