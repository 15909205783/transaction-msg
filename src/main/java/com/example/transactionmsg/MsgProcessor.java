package com.example.transactionmsg;

import com.example.transactionmsg.Util.DB;
import com.example.transactionmsg.common.SystemEnvType;
import com.example.transactionmsg.hook.SendTXMsgHook;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsgProcessor {

  private static final int getTimeoutMs = 100;
  private static final int[] timeOutData = new int[]{0, 5, 10, 25, 50, 100, 200, 300, 500, 800, 1000, 1000, 1000};
  private static final int maxDealTime;
  private static final int limitNum = 50;
  private static final int maxDealNumOneTime = 2000;
  private static final int timeWheelPeriod = 5;
  private static String[] defaultEtcd;
  private static final int holdLockTime = 60;
  private static final int sanboxTimes = 3;
  private static final Logger log;
  private PriorityBlockingQueue<Msg> msgQueue;
  private ExecutorService exeService;
  private PriorityBlockingQueue<Msg> timeWheelQueue;
  private ScheduledExecutorService scheService;
  private AtomicReference<State> state;
  private DefaultMQProducer producer;
  private MsgStorage msgStorage;
  private Config config;
  private volatile boolean holdLock = true;
  private String lockKey = "defaultTransKey";
  private SystemEnvType systemEnvType = null;
  private List<SendTXMsgHook> hookList;

  private RedissonClient redissonClient;

  Producer<byte[]> pulsarProducer;

  static {
    maxDealTime = timeOutData.length;
    log = LoggerFactory.getLogger(MsgProcessor.class);
  }

  public MsgProcessor(DefaultMQProducer producer, Producer<byte[]> pulsarProducer, MsgStorage msgStorage) {
    this.producer = producer;
    this.pulsarProducer = pulsarProducer;
    this.msgStorage = msgStorage;
    this.msgQueue = new PriorityBlockingQueue(5000, new Comparator<Msg>() {
      public int compare(Msg o1, Msg o2) {
        long diff = o1.getCreateTime() - o2.getCreateTime();
        if (diff > 0L) {
          return 1;
        } else {
          return diff < 0L ? -1 : 0;
        }
      }
    });
    this.timeWheelQueue = new PriorityBlockingQueue(1000, new Comparator<Msg>() {
      public int compare(Msg o1, Msg o2) {
        long diff = o1.getNextExpireTime() - o2.getNextExpireTime();
        if (diff > 0L) {
          return 1;
        } else {
          return diff < 0L ? -1 : 0;
        }
      }
    });
    this.state = new AtomicReference(State.CREATE);
  }

  public void init(final Config config, final RedissonClient redissonClient) {
    this.redissonClient = redissonClient;

    if (this.state.get().equals(State.RUNNING)) {
      log.info("[ARCH_TXMQ_INIT] Msg Processor have inited return");
    } else {
      log.info("[ARCH_TXMQ_INIT] MsgProcessor init start");
      this.state.compareAndSet(State.CREATE, State.RUNNING);
      this.systemEnvType = SystemEnvUtil.getSysEnv();
      if (config.getEtcdHosts() != null && config.getEtcdHosts().length >= 1) {
        defaultEtcd = config.getEtcdHosts();
      }

      log.info("[ARCH_TXMQ_INIT] serverType {} etcdhosts {}", this.systemEnvType, defaultEtcd);
      this.config = config;
      if (SystemEnvType.TEST.equals(this.systemEnvType)) {
        this.config.setDeleteTimePeriod(this.config.getDeleteTimePeriod() * 3);
        this.config.setSchedScanTimePeriod(this.config.getSchedScanTimePeriod() * 3);
      }

      this.exeService = Executors.newFixedThreadPool(config.getThreadNum(), new ThreadFactory() {
        int thread_id = 0;

        public Thread newThread(Runnable r) {
          return new Thread(r, "MsgProcessorThread-" + this.thread_id++);
        }
      });
      this.scheService = Executors.newScheduledThreadPool(config.getSchedThreadNum(), new ThreadFactory() {
        int thread_id = 0;

        public Thread newThread(Runnable r) {
          return new Thread(r, "MsgScheduledThread-" + this.thread_id++);
        }
      });

      for (int i = 0; i < config.getThreadNum(); ++i) {
        MsgProcessor.MsgProcessorRunnable runnable = new MsgProcessor.MsgProcessorRunnable();
        this.exeService.submit(runnable);
      }

      this.scheService.scheduleAtFixedRate(new MsgProcessor.MsgTimeWheelRunnable(), 5L, 5L, TimeUnit.MILLISECONDS);
      this.scheService.scheduleAtFixedRate(new ScheduleScanMsgRunnable(), config.schedScanTimePeriod,
          config.schedScanTimePeriod, TimeUnit.SECONDS);
      this.scheService.scheduleAtFixedRate(new MsgProcessor.DeleteMsgRunnable(), config.deleteTimePeriod,
          config.deleteTimePeriod, TimeUnit.SECONDS);
      this.scheService.scheduleAtFixedRate(() -> {
        MsgProcessor.log.info("[ARCH_TXMQ_STATE] stats info msgQueue size {} timeWheelQueue size {}",
            MsgProcessor.this.msgQueue.size(), MsgProcessor.this.timeWheelQueue.size());
        if (config.printTableState()) {
          try {
            Map<String, Long> tableCount = MsgProcessor.this.msgStorage.selectTableCount();
            Iterator<Entry<String, Long>> iterator = tableCount.entrySet().iterator();

            while (iterator.hasNext()) {
              Entry<String, Long> entry = iterator.next();
              String tag = Optional.ofNullable(Util.getDataBase(entry.getKey())).map(DB::getDataBase).orElse("");
              MsgProcessor.log.info("[ARCH_TXMQ_STATE] the mq message db[{}] table's count is {}", tag,
                  entry.getValue());
            }
          } catch (SQLException e) {
            MsgProcessor.log.error("[ARCH_TXMQ_STATE] select mq message count has exception", e);
          }

        }
      }, 20L, config.getStatsTimePeriod(), TimeUnit.SECONDS);
      this.initZZlock();
      log.info("[ARCH_TXMQ_INIT] MsgProcessor init end");
    }
  }

  private void initZZlock() {
    boolean userDefineEtcd = this.config.getEtcdHosts() != null && this.config.getEtcdHosts().length > 0;
    if (this.systemEnvType == SystemEnvType.ONLINE || userDefineEtcd) {
      List<DBDataSource> list = this.msgStorage.getDbDataSources();
      String clusterName = this.config.getServiceName();
      if (list != null && list.size() > 0) {
        String url = list.get(0).getUrl();
        DB db = Util.getDataBase(url);
        clusterName = clusterName + "with" + db.getDataBase();
        String key = db.buildKey();
        if (key != null && key.length() > 0) {
          this.lockKey = key;
        }
      }

      this.lockKey = this.lockKey + this.msgStorage.getTopicListUniqueKey();
      log.info("[ARCH_TXMQ_INIT] init zzlock clustername {} lockKey {} etcdString {}", clusterName, this.lockKey,
          Arrays.toString(defaultEtcd));
    }

  }

  public void close() {
    log.info("[ARCH_TXMQ_CLOSE] start close msgProcessor ");
    this.state.compareAndSet(State.RUNNING, State.CLOSED);

    try {
      this.exeService.shutdown();
      this.scheService.shutdown();
      this.exeService.awaitTermination(this.config.getCloseWaitTime(), TimeUnit.MILLISECONDS);
      this.scheService.awaitTermination(this.config.getCloseWaitTime(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log.error("[ARCH_TXMQ_CLOSE] close MsgProcessor error {}", e.getMessage());
    }

    if (!this.exeService.isShutdown()) {
      this.exeService.shutdownNow();
    }

    if (!this.scheService.isShutdown()) {
      this.scheService.shutdownNow();
    }

    log.info("[ARCH_TXMQ_CLOSE] close msgProcessor success");
  }

  public void registerSendHookList(List<SendTXMsgHook> hookList) {
    if (hookList != null) {
      this.hookList = hookList;
      List<String> hookClassList = hookList.stream().map(Object::getClass).map(Class::getName)
          .collect(Collectors.toList());
      log.info("[ARCH_TXMQ_INIT] register send hook list success {}", hookClassList);
    }

  }

  protected void putMsg(Msg msg) {
    this.msgQueue.put(msg);
  }

  private static Message buildMsg(MsgInfo msgInfo) throws UnsupportedEncodingException {
    String topic = msgInfo.getTopic();
    String tag = msgInfo.getTag();
    String content = msgInfo.getContent();
    String id = msgInfo.getId() + "";
    String propertiesStr = msgInfo.getPropertiesStr();
    Message msg = new Message(topic, tag, id, content.getBytes("UTF-8"));
    if (propertiesStr != null && !propertiesStr.isEmpty()) {
      Map<String, String> properties = MessageDecoder.string2messageProperties(propertiesStr);
      Iterator<Entry<String, String>> iterator = properties.entrySet().iterator();

      while (iterator.hasNext()) {
        Entry<String, String> entry = iterator.next();
        msg.putUserProperty(entry.getKey(), entry.getValue());
      }
    }

    String header = String.format("{\"topic\":\"%s\",\"tag\":\"%s\",\"id\":\"%s\",\"createTime\":\"%s\"}", topic, tag,
        id, System.currentTimeMillis());
    msg.putUserProperty("MQHeader", header);
    Date create_time = msgInfo.getCreate_time();
    long create_ms = create_time.getTime();
    int diff = (int) ((System.currentTimeMillis() - create_ms) / 1000L);
    int latestDelay = msgInfo.getDelay() - diff;
    if (latestDelay > 0) {
//      msg.setDelayTime(latestDelay, TimeUnit.SECONDS);
    }

    return msg;
  }

  private void executeHookAfterSendSuccess(Message message, SendResult sendResult) {
    if (this.hookList != null) {
      Iterator<SendTXMsgHook> iterator = this.hookList.iterator();

      while (iterator.hasNext()) {
        SendTXMsgHook hook = iterator.next();

        try {
          String interestTopic = hook.interestedTopic();
          Set<String> interestTag = hook.interestedTag();
          if ((interestTopic == null || Objects.equals(message.getTopic(), interestTopic)) && (interestTag == null
              || interestTag.contains(message.getTags()))) {
            hook.afterSendSuccess(message, sendResult);
          }
        } catch (Throwable e) {
          log.warn("[ARCH_TXMQ_HOOK] execute Hook has exception", e);
        }
      }

    }
  }


  class ScheduleScanMsgRunnable implements Runnable {

    ScheduleScanMsgRunnable() {
    }

    public void run() {
      if (MsgProcessor.this.state.get().equals(State.RUNNING)) {
        HashMap<String, DataSource> data = MsgProcessor.this.msgStorage.getDataSourcesMap();
        Collection<DataSource> dataSrcList = data.values();
        Iterator<DataSource> iterator = dataSrcList.iterator();

        while (iterator.hasNext()) {
          DataSource dataSrc = iterator.next();
          if (redissonClient == null) {
            this.doExecute(dataSrc);
          } else {
            RLock lock = redissonClient.getLock(lockKey);
            boolean locked = lock.tryLock();
            if (locked) {
              try {
                this.doExecute(dataSrc);
              } finally {
                if (lock.isLocked()) {
                  lock.unlock();
                }
              }
            } else {
              log.info("定时任务扫描消息日志表处理消息，加锁失败！！");
            }
          }
        }
      }

    }

    private void doExecute(DataSource dataSrc) {
      MsgProcessor.log.info("[ARCH_TXMQ_SCANNER] SchedScanMsgRunnable run");
      int num = 50;
      int count = 0;

      while (num == 50 && count < 2000) {
        List<MsgInfo> list = null;
        try {
          list = MsgProcessor.this.msgStorage.getWaitingMsg(dataSrc, 50);
        } catch (SQLException e) {
          e.printStackTrace();
        }
        num = list.size();
        if (num > 0) {
          MsgProcessor.log.debug("[ARCH_TXMQ_SCANNER] scan db get msg size {} ", num);
        }

        count += num;
        Iterator<MsgInfo> iterator = list.iterator();

        while (iterator.hasNext()) {
          MsgInfo msgInfo = iterator.next();

          try {
            Message mqMsg = MsgProcessor.buildMsg(msgInfo);
            SendResult result = MsgProcessor.this.pulsarProducer.send(mqMsg);
            MsgProcessor.log.info("[ARCH_TXMQ_SCANNER] msgId {} topic {} sendMsg result {}", msgInfo.getId(),
                mqMsg.getTopic(), result);
            if (result != null && result.getSendStatus() == SendStatus.SEND_OK) {
              int res = MsgProcessor.this.msgStorage.deleteMsgByID(dataSrc, msgInfo.getId());
              MsgProcessor.log.debug("[ARCH_TXMQ_SCANNER] msgId {} deleteMsgByID success res {}", msgInfo.getId(), res);
              MsgProcessor.this.executeHookAfterSendSuccess(mqMsg, result);
            }
          } catch (Exception e) {
            MsgProcessor.log.error("[ARCH_TXMQ_SCANNER] SchedScanMsg deal fail", e);
          }
        }
      }

    }
  }

  class DeleteMsgRunnable implements Runnable {

    DeleteMsgRunnable() {
    }

    public boolean isTimeUp() {
      Calendar instance = Calendar.getInstance();
      int hour = instance.get(11);
      return hour == 3 || hour == 4;
    }

    public void run() {
      if (MsgProcessor.this.state.get().equals(State.RUNNING)) {
        if (!this.isTimeUp()) {
          return;
        }

        try {
          HashMap<String, DataSource> data = MsgProcessor.this.msgStorage.getDataSourcesMap();
          Collection<DataSource> dataSrcList = data.values();
          Iterator<DataSource> iterator = dataSrcList.iterator();

          while (iterator.hasNext()) {
            DataSource dataSrc = iterator.next();
            MsgProcessor.log.info("[ARCH_TXMQ_MSG_DELETER] DeleteMsgRunnable run ");
            int count = 0;
            int num = MsgProcessor.this.config.deleteMsgOneTimeNum;

            while (MsgProcessor.this.config.deleteMsgOneTimeNum > 0
                && num == MsgProcessor.this.config.deleteMsgOneTimeNum && count < 2000) {
              List<Long> msgID = MsgProcessor.this.msgStorage.getHistoryMsgID(dataSrc,
                  MsgProcessor.this.config.deleteMsgOneTimeNum);
              num = msgID.size();
              if (!msgID.isEmpty()) {
                num = MsgProcessor.this.msgStorage.deleteMsgByIDList(dataSrc,
                    MsgProcessor.this.config.deleteMsgOneTimeNum, msgID.toArray(new Long[0]));
              }

              count += num;
            }

            MsgProcessor.log.info(
                "[ARCH_TXMQ_MSG_DELETER] DeleteMsgRunnable end run, delete history msg before {}, delete num={}",
                MsgProcessor.this.config.getStoreDayBeforeTimeStamp(), count);
          }
        } catch (Exception e) {
          MsgProcessor.log.error("[ARCH_TXMQ_MSG_DELETER] delete Run error ", e);
        }
      }

    }
  }

  class MsgTimeWheelRunnable implements Runnable {

    MsgTimeWheelRunnable() {
    }

    public void run() {
      try {
        if (MsgProcessor.this.state.get().equals(State.RUNNING)) {
          long cruTime = System.currentTimeMillis();

          for (Msg msg = MsgProcessor.this.timeWheelQueue.peek();
              msg != null && msg.getNextExpireTime() <= cruTime; msg = MsgProcessor.this.timeWheelQueue.peek()) {
            msg = MsgProcessor.this.timeWheelQueue.poll();
            MsgProcessor.log.debug("timeWheel poll msg ,return to msgQueue {}", msg);
            MsgProcessor.this.msgQueue.put(msg);
          }
        }
      } catch (Exception e) {
        MsgProcessor.log.error("pool timequeue error", e);
      }

    }
  }

  class MsgProcessorRunnable implements Runnable {

    MsgProcessorRunnable() {
    }

    public void run() {
      log.info("---------MsgProcessorRunnable-----------");
      while (MsgProcessor.this.state.get().equals(State.RUNNING)) {
        Msg msg = null;

        try {
          try {
            msg = MsgProcessor.this.msgQueue.poll(100L, TimeUnit.MILLISECONDS);

          } catch (InterruptedException e) {
            MsgProcessor.log.error("消息出队列失败，{}", e);
          }

          if (msg != null) {
            int msgSize = MsgProcessor.this.msgQueue.size() + MsgProcessor.this.timeWheelQueue.size();
            if (msgSize > MsgProcessor.this.config.getMaxMemoryMsgCount()) {
              MsgProcessor.log.info(
                  "[ARCH_TXMQ_PROCESSOR] There are too many messages in memory, this msg[{}] will send by db scanner thread. msgQueueSize={} timeWheelQueue={} maxMemoryMsgCount={}",
                  msg, MsgProcessor.this.msgQueue.size(), MsgProcessor.this.timeWheelQueue.size(),
                  MsgProcessor.this.config.getMaxMemoryMsgCount());
            } else {
              MsgProcessor.log.debug("[ARCH_TXMQ_PROCESSOR] poll msg {}", msg);
              int dealedTime = msg.getHaveDealedTimes() + 1;
              msg.setHaveDealedTimes(dealedTime);
              MsgInfo msgInfo = MsgProcessor.this.msgStorage.getMsgById(msg);
              MsgProcessor.log.info("[ARCH_TXMQ_PROCESSOR] getMsgInfo from DB {}", msgInfo);
              if (msgInfo == null) {
                if (dealedTime < MsgProcessor.maxDealTime) {
                  long nextExpireTime = System.currentTimeMillis() + (long) MsgProcessor.timeOutData[dealedTime];
                  msg.setNextExpireTime(nextExpireTime);
                  MsgProcessor.this.timeWheelQueue.put(msg);
                  MsgProcessor.log.debug("[ARCH_TXMQ_PROCESSOR] put msg in timeWhellQueue {} ", msg);
                } else {
                  MsgProcessor.log.info(
                      "[ARCH_TXMQ_PROCESSOR] msg wait timeout in memory because transaction has not commit, this msg[{}] will send by db scanner thread. ",
                      msg);
                }
              } else {
                msgInfo.setCreate_time(new Date(msg.getCreateTime()));
                Message mqMsg = MsgProcessor.buildMsg(msgInfo);
                MsgProcessor.log.debug("[ARCH_TXMQ_PROCESSOR] will sendMsg {}", mqMsg);
                SendResult result = MsgProcessor.this.producer.send(mqMsg);

               final MessageId messageId = MsgProcessor.this.pulsarProducer.send(
                    mqMsg.toString().getBytes(StandardCharsets.UTF_8));

                log.info("Pulsar消息发送成功,messageId: {}", messageId.toString());

                MsgProcessor.log.info(
                    "[ARCH_TXMQ_PROCESSOR] msgId {} topic {} haveDealedTimes={}, createTime={} sendMsg result {}",
                    msgInfo.getId(), mqMsg.getTopic(), msg.getHaveDealedTimes(), msg.getCreateTime(), result);
                if (null != result && result.getSendStatus() == SendStatus.SEND_OK) {
                  if (result.getSendStatus() == SendStatus.SEND_OK) {
                    int res = MsgProcessor.this.msgStorage.deleteMsgByID(msg.getDb(), msg.getId());
                    MsgProcessor.log.debug("[ARCH_TXMQ_PROCESSOR] msgId {} deleteMsgByID success res {}",
                        msgInfo.getId(), res);
                    MsgProcessor.this.executeHookAfterSendSuccess(mqMsg, result);
                  }
                } else if (dealedTime < MsgProcessor.maxDealTime) {
                  long nextExpireTimex = System.currentTimeMillis() + (long) MsgProcessor.timeOutData[dealedTime];
                  msg.setNextExpireTime(nextExpireTimex);
                  MsgProcessor.this.timeWheelQueue.put(msg);
                  MsgProcessor.log.debug("[ARCH_TXMQ_PROCESSOR] put msg in timeWhellQueue {} ", msg);
                }
              }
            }
          }
        } catch (Throwable e) {
          MsgProcessor.log.error("[ARCH_TXMQ_PROCESSOR] MsgProcessor deal msg fail", e);
        }
      }

    }
  }
}
