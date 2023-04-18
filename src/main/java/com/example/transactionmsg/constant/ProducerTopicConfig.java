package com.example.transactionmsg.constant;

import java.util.List;
import org.apache.pulsar.client.api.CompressionType;

public class ProducerTopicConfig {

  /** 是否开启生产者订阅 */
  private boolean enabled = true;

  /** 发送超时时间 单位毫秒 */
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
  private CompressionType compressionType = CompressionType.NONE;

  /** 订阅主题配置 */
  private List<TopicProperties> topics;
}
