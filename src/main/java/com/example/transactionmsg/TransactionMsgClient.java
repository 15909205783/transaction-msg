package com.example.transactionmsg;

import com.example.transactionmsg.Util.DB;
import com.example.transactionmsg.common.ApplicationUtil;
import com.example.transactionmsg.common.TXMQVersion;
import com.example.transactionmsg.constant.ClientInitException;
import com.example.transactionmsg.hook.SendTXMsgHook;
import com.google.common.base.Strings;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TransactionMsgClient {

  protected static final Logger log = LoggerFactory.getLogger(TransactionMsgClient.class);

  private final String producerName;
  private final String appName;
  private static final int minDelay = 0;
  private static final int maxDelay = 7776000;

  private DefaultMQProducer producer;
  private String mqAddr;
  private List<DBDataSource> dbDataSources;
  protected MsgProcessor msgProcessor;
  private MsgStorage msgStorage;
  private Config config;
  private AtomicReference<State> state;
  private String localIp;
  private List<SendTXMsgHook> sendHookList;
  private RedissonClient redissonClient;

  private Producer<byte[]> pulsarProducer;


  protected TransactionMsgClient(RedissonClient redissonClient, String mqAddr, List<DBDataSource> dbDataSources,
      List<String> topicLists, Config config) throws PulsarClientException, ClientInitException {
    TXMQVersion.setCurrentVersionProp();

    PulsarClient pulsarClient = pulsarClient(config);
    this.pulsarProducer = pulsarClient.newProducer()
        //topic完整路径，格式为persistent://集群（租户）ID/命名空间/Topic名称
        .topic("persistent://pulsar-jegz795k7km7/wms_test/PAAS_GET_SHEET")
        .create();


    this.mqAddr = mqAddr;
    this.dbDataSources = dbDataSources;
    this.appName = ApplicationUtil.getApplicationName();
    this.msgStorage = new MsgStorage(dbDataSources, topicLists);
    this.producer = new DefaultMQProducer();
    this.producerName = this.buildProducerGroupName();
    this.producer.setProducerGroup(this.producerName);
    this.producer.setNamesrvAddr(this.mqAddr);
    this.msgProcessor = new MsgProcessor(this.producer, this.pulsarProducer, this.msgStorage);
    this.config = config;
    this.state = new AtomicReference(State.CREATE);
    this.redissonClient = redissonClient;
  }

  public abstract Long sendMsg(String content, String topic) throws Exception;

  public abstract Long sendMsg(String content, String topic, String tag) throws Exception;

  public abstract Long sendMsg(String content, String topic, String tag, int delay) throws Exception;

  public void init() throws Exception {
    if (this.state.get().equals(State.RUNNING)) {
      log.info("[ARCH_TXMQ_INIT] TransactionMsgClient have inited, return");
    } else {
      log.info("[ARCH_TXMQ_INIT] start init mqAddr={} state {} this {}", this.mqAddr, this.state, this);
//      this.producer.setSendMsgTimeout(this.config.getSendMsgTimeout());
      if (this.config == null) {
        this.config = new Config();
      }

      try {
//        this.producer.start();
        this.msgProcessor.init(this.config, this.redissonClient);
        this.msgProcessor.registerSendHookList(this.sendHookList);
        this.msgStorage.init(this.config);
        this.localIp = SystemEnvUtil.getIp();
        log.info("[ARCH_TXMQ_INIT] end init success, the tx msg version is {}, mq version is {}",
            TXMQVersion.CURRENT_VERSION, MQVersion.value2Version(MQVersion.CURRENT_VERSION));
      } catch (Exception e) {
        log.error("producer start fail", e);
        throw e;
      }

      this.state.compareAndSet(State.CREATE, State.RUNNING);
    }
  }

  public void close() {
    log.info("start close TransactionMsgClient");
    if (this.state.compareAndSet(State.RUNNING, State.CLOSED)) {
      this.msgProcessor.close();
      this.msgStorage.close();
      this.producer.shutdown();
    } else {
      log.info("state not right {} ", this.state);
    }

  }

  public Long sendMsg(Connection con, String content, String topic, String tag, int delay) throws Exception {
    Long id = null;
    if (!this.state.get().equals(State.RUNNING)) {
      log.error("TransactionMsgClient not Running , please call init function");
      throw new Exception("TransactionMsgClient not Running , please call init function");
    } else if (content != null && !content.isEmpty() && topic != null && !topic.isEmpty()) {
      if (!this.msgStorage.isInTopicLists(topic)) {
        log.error("wan't to send msg in topic " + topic
            + " which is not in topicLists of config, can't resend if send failed");
        throw new Exception("wan't to send msg in topic " + topic
            + " which is not in topicLists of config, can't resend if send failed");
      } else if (delay >= 0 && delay <= 7776000) {

        try {
          if (con.isClosed() || con.getAutoCommit()) {
            throw new Exception("send tx msg but connection not in transaction.");
          }

          Message message = new Message();
          MessageAccessor.setProperties(message, new HashMap());

          Entry<Long, DB> idUrlPair = MsgStorage.insertMsg(con, content, topic, tag, delay, message.getProperties());

          id = idUrlPair.getKey();
          Msg msg = new Msg(id, idUrlPair.getValue(), topic);
          this.msgProcessor.putMsg(msg);

        } catch (Exception e) {

          log.error("sendMsg fail topic {} tag {} ", new Object[]{topic, tag, e});
          throw e;
        }
      }

      return id;
    } else {
      log.error("delay can't <0 or > 7776000");
      throw new Exception("delay can't <0 or > 7776000");
    }

  }

  private String buildProducerGroupName() {
    String tmpGroup;
    if (this.appName != null && !this.appName.isEmpty()) {
      tmpGroup = "TransactionMsgProducer-" + this.appName;
    } else {
      tmpGroup = "TransactionMsgProducer-" + this.producer.buildMQClientId();
    }

    Matcher matcher = Util.PATTERN.matcher(tmpGroup);
    StringBuffer groupBuffer = new StringBuffer();

    while (matcher.find()) {
      groupBuffer.append(matcher.group());
    }

    return groupBuffer.toString();
  }

  public String getMqAddr() {
    return this.mqAddr;
  }

  public void setMqAddr(String mqAddr) {
    this.mqAddr = mqAddr;
  }

  public List<DBDataSource> getDbDataSources() {
    return this.dbDataSources;
  }

  public void setDbDataSources(List<DBDataSource> dbDataSources) {
    this.dbDataSources = dbDataSources;
  }

  public Config getConfig() {
    return this.config;
  }

  public void setConfig(Config config) {
    this.config = config;
  }

  public void registerSendHook(SendTXMsgHook hook) {
    if (this.state.get().equals(State.RUNNING)) {
      throw new RuntimeException(
          "[ARCH_TXMQ_INIT] TransactionMsgClient has inited, can't register TXMsgSendSuccessListener");
    } else {
      if (this.sendHookList == null) {
        this.sendHookList = new ArrayList();
      }

      this.sendHookList.add(hook);
    }
  }

  public void setSendHookList(List<SendTXMsgHook> sendHookList) {
    if (this.state.get().equals(State.RUNNING)) {
      throw new RuntimeException(
          "[ARCH_TXMQ_INIT] TransactionMsgClient has inited, can't register TXMsgSendSuccessListener");
    } else {
      this.sendHookList = sendHookList;
    }
  }


  public PulsarClient pulsarClient(Config config) throws PulsarClientException, ClientInitException {
    if (!Strings.isNullOrEmpty(config.getTlsAuthCertFilePath())
        && !Strings.isNullOrEmpty(config.getTlsAuthKeyFilePath())
        && !Strings.isNullOrEmpty(config.getTokenAuthValue())) {
      throw new ClientInitException("You cannot use multiple auth options.");
    }

    final ClientBuilder pulsarClientBuilder =
        PulsarClient.builder()
            .serviceUrl(config.getServiceUrl())
            .ioThreads(config.getIoThreads())
            .listenerThreads(config.getListenerThreads())
            .enableTcpNoDelay(config.isEnableTcpNoDelay())
            .keepAliveInterval(config.getKeepAliveIntervalSec(), TimeUnit.SECONDS)
            .connectionTimeout(config.getConnectionTimeoutSec(), TimeUnit.SECONDS)
            .operationTimeout(config.getOperationTimeoutSec(), TimeUnit.SECONDS)
            .startingBackoffInterval(config.getStartingBackoffIntervalMs(), TimeUnit.MILLISECONDS)
            .maxBackoffInterval(config.getMaxBackoffIntervalSec(), TimeUnit.SECONDS)
            .useKeyStoreTls(config.isUseKeyStoreTls())
            .tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath())
            .tlsCiphers(config.getTlsCiphers())
            .tlsProtocols(config.getTlsProtocols())
            .tlsTrustStorePassword(config.getTlsTrustStorePassword())
            .tlsTrustStorePath(config.getTlsTrustStorePath())
            .tlsTrustStoreType(config.getTlsTrustStoreType())
            .allowTlsInsecureConnection(config.isAllowTlsInsecureConnection())
            .enableTlsHostnameVerification(config.isEnableTlsHostnameVerification());

    if (!Strings.isNullOrEmpty(config.getTlsAuthCertFilePath())
        && !Strings.isNullOrEmpty(config.getTlsAuthKeyFilePath())) {
      pulsarClientBuilder.authentication(
          AuthenticationFactory.TLS(config.getTlsAuthCertFilePath(), config.getTlsAuthKeyFilePath()));
    }

    if (!Strings.isNullOrEmpty(config.getTokenAuthValue())) {
      pulsarClientBuilder.authentication(
          AuthenticationFactory.token(config.getTokenAuthValue()));
    }

    return pulsarClientBuilder.build();
  }
}
