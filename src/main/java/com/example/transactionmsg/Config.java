package com.example.transactionmsg;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class Config {
  int threadNum = 10;
  int deleteTimePeriod = 180;
  int deleteMsgOneTimeNum = 200;
  int schedThreadNum = 6;
  int minIdleConnectionNum = 6;
  int maxActiveConnectionNum = 20;
  int sendMsgTimeout = 3000;
  int schedScanTimePeriod = 120;
  int maxWaitTime = 6000;
  int closeWaitTime = 5000;
  int statsTimePeriod = 300;
  /** @deprecated */
  @Deprecated
  int historyMsgStoreTime = 3;
  int historyMsgStoreDay = 7;
  int maxScanSecondsAgo = 86400;
  int maxMemoryMsgCount = 1500000;
  int minScanSecondsAgo = 10;
  String msgTableName = "mq_messages";
  String sandboxMsgTableName = "uat_mq_messages";
  String[] etcdHosts;
  String serviceName = "transmsg";
  String printTableState = "true";



  private String serviceUrl = "http://pulsar-jegz795k7km7.sap-m46ixay0.tdmq.ap-gz.public.tencenttdmq.com:8080";
  private Integer ioThreads = 10;
  private Integer listenerThreads = 10;
  private boolean enableTcpNoDelay = false;
  private Integer keepAliveIntervalSec = 20;
  private Integer connectionTimeoutSec = 10;
  private Integer operationTimeoutSec = 15;
  private Integer startingBackoffIntervalMs = 100;
  private Integer maxBackoffIntervalSec = 10;
  private String consumerNameDelimiter = "";
  private String namespace = "default";
  private String tenant = "public";
  private String tlsTrustCertsFilePath = null;
  private Set<String> tlsCiphers = new HashSet<>();
  private Set<String> tlsProtocols = new HashSet<>();
  private String tlsTrustStorePassword = null;
  private String tlsTrustStorePath = null;
  private String tlsTrustStoreType = null;
  private boolean useKeyStoreTls = false;
  private boolean allowTlsInsecureConnection = false;
  private boolean enableTlsHostnameVerification = false;
  private String tlsAuthCertFilePath = null;
  private String tlsAuthKeyFilePath = null;
  private String tokenAuthValue = null;

  /**
   * 1-逻辑删除 2物理删除
   */
  private Integer deleteType = 1;




  public Config() {
  }

  public String getSandboxMsgTableName() {
    return this.sandboxMsgTableName;
  }

  public void setSandboxMsgTableName(String sandBoxMsgTableName) {
    this.sandboxMsgTableName = sandBoxMsgTableName;
  }

  public int getHistoryMsgStoreDay() {
    return this.historyMsgStoreDay;
  }

  public int getMaxMemoryMsgCount() {
    return this.maxMemoryMsgCount;
  }

  public void setMaxMemoryMsgCount(int maxMemoryMsgCount) {
    this.maxMemoryMsgCount = maxMemoryMsgCount;
  }

  public void setHistoryMsgStoreDay(int historyMsgStoreDay) {
    this.historyMsgStoreDay = historyMsgStoreDay;
  }

  public String getPrintTableState() {
    return this.printTableState;
  }

  public void setPrintTableState(String printTableState) {
    this.printTableState = printTableState;
  }

  public boolean printTableState() {
    return Objects.equals(this.printTableState, "true");
  }

  public int getThreadNum() {
    return this.threadNum;
  }

  public void setThreadNum(int threadNum) {
    this.threadNum = threadNum;
  }

  public int getDeleteTimePeriod() {
    return this.deleteTimePeriod;
  }

  public void setDeleteTimePeriod(int deleteTimePeriod) {
    this.deleteTimePeriod = deleteTimePeriod;
  }

  public int getDeleteMsgOneTimeNum() {
    return this.deleteMsgOneTimeNum;
  }

  public void setDeleteMsgOneTimeNum(int deleteMsgOneTimENum) {
    this.deleteMsgOneTimeNum = deleteMsgOneTimENum;
  }

  public int getSchedThreadNum() {
    return this.schedThreadNum;
  }

  public void setSchedThreadNum(int schedThreadNum) {
    this.schedThreadNum = schedThreadNum;
  }

  public int getMinIdleConnectionNum() {
    return this.minIdleConnectionNum;
  }

  public void setMinIdleConnectionNum(int minIdleConnectionNum) {
    this.minIdleConnectionNum = minIdleConnectionNum;
  }

  public int getMaxActiveConnectionNum() {
    return this.maxActiveConnectionNum;
  }

  public void setMaxActiveConnectionNum(int maxActiveConnectionNum) {
    this.maxActiveConnectionNum = maxActiveConnectionNum;
  }

  public int getSendMsgTimeout() {
    return this.sendMsgTimeout;
  }

  public void setSendMsgTimeout(int sendMsgTimeout) {
    this.sendMsgTimeout = sendMsgTimeout;
  }

  public int getSchedScanTimePeriod() {
    return this.schedScanTimePeriod;
  }

  public void setSchedScanTimePeriod(int schedScanTimePeriod) {
    this.schedScanTimePeriod = schedScanTimePeriod;
  }

  public int getMaxWaitTime() {
    return this.maxWaitTime;
  }

  public void setMaxWaitTime(int maxWaitTime) {
    this.maxWaitTime = maxWaitTime;
  }

  public int getCloseWaitTime() {
    return this.closeWaitTime;
  }

  public void setCloseWaitTime(int closeWaitTime) {
    this.closeWaitTime = closeWaitTime;
  }

  int getStatsTimePeriod() {
    return this.statsTimePeriod;
  }

  void setStatsTimePeriod(int statsTimePeriod) {
    this.statsTimePeriod = statsTimePeriod;
  }

  public int getHistoryMsgStoreTime() {
    return this.historyMsgStoreTime;
  }

  public void setHistoryMsgStoreTime(int historyMsgStoreTime) {
    this.historyMsgStoreTime = historyMsgStoreTime;
  }

  public String getMsgTableName() {
    return this.msgTableName;
  }

  public void setMsgTableName(String msgTableName) {
    this.msgTableName = msgTableName;
  }

  public String[] getEtcdHosts() {
    return this.etcdHosts;
  }

  public void setEtcdHosts(String[] etcdHosts) {
    this.etcdHosts = etcdHosts;
  }

  public String getServiceName() {
    return this.serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public void setMaxScanSecondsAgo(int maxScanSecondsAgo) {
    this.maxScanSecondsAgo = maxScanSecondsAgo;
  }

  public void setMinScanSecondsAgo(int minScanSecondsAgo) {
    this.minScanSecondsAgo = minScanSecondsAgo;
  }

  public int getMaxScanSecondsAgo() {
    return this.maxScanSecondsAgo;
  }

  public int getMinScanSecondsAgo() {
    return this.minScanSecondsAgo;
  }

  Timestamp getScanBeginTimestamp() {
    long time = System.currentTimeMillis() - (long)(this.maxScanSecondsAgo * 1000);
    Timestamp timestamp = new Timestamp(time);
    return timestamp;
  }

  Timestamp getScanEndTimestamp() {
    long time = System.currentTimeMillis() - (long)(this.minScanSecondsAgo * 1000);
    Timestamp timestamp = new Timestamp(time);
    return timestamp;
  }

  Timestamp getStoreDayBeforeTimeStamp() {
    long time = System.currentTimeMillis() - (long)(this.historyMsgStoreDay * 24 * 60 * 60 * 1000);
    Timestamp timestamp = new Timestamp(time);
    return timestamp;
  }

  //pulsar config begin

  public String getServiceUrl() {
    return serviceUrl;
  }

  public void setServiceUrl(String serviceUrl) {
    this.serviceUrl = serviceUrl;
  }

  public Integer getIoThreads() {
    return ioThreads;
  }

  public void setIoThreads(Integer ioThreads) {
    this.ioThreads = ioThreads;
  }

  public Integer getListenerThreads() {
    return listenerThreads;
  }

  public void setListenerThreads(Integer listenerThreads) {
    this.listenerThreads = listenerThreads;
  }

  public boolean isEnableTcpNoDelay() {
    return enableTcpNoDelay;
  }

  public void setEnableTcpNoDelay(boolean enableTcpNoDelay) {
    this.enableTcpNoDelay = enableTcpNoDelay;
  }

  public Integer getKeepAliveIntervalSec() {
    return keepAliveIntervalSec;
  }

  public void setKeepAliveIntervalSec(Integer keepAliveIntervalSec) {
    this.keepAliveIntervalSec = keepAliveIntervalSec;
  }

  public Integer getConnectionTimeoutSec() {
    return connectionTimeoutSec;
  }

  public void setConnectionTimeoutSec(Integer connectionTimeoutSec) {
    this.connectionTimeoutSec = connectionTimeoutSec;
  }

  public Integer getOperationTimeoutSec() {
    return operationTimeoutSec;
  }

  public void setOperationTimeoutSec(Integer operationTimeoutSec) {
    this.operationTimeoutSec = operationTimeoutSec;
  }

  public Integer getStartingBackoffIntervalMs() {
    return startingBackoffIntervalMs;
  }

  public void setStartingBackoffIntervalMs(Integer startingBackoffIntervalMs) {
    this.startingBackoffIntervalMs = startingBackoffIntervalMs;
  }

  public Integer getMaxBackoffIntervalSec() {
    return maxBackoffIntervalSec;
  }

  public void setMaxBackoffIntervalSec(Integer maxBackoffIntervalSec) {
    this.maxBackoffIntervalSec = maxBackoffIntervalSec;
  }

  public String getConsumerNameDelimiter() {
    return consumerNameDelimiter;
  }

  public void setConsumerNameDelimiter(String consumerNameDelimiter) {
    this.consumerNameDelimiter = consumerNameDelimiter;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public String getTlsTrustCertsFilePath() {
    return tlsTrustCertsFilePath;
  }

  public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
    this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
  }

  public Set<String> getTlsCiphers() {
    return tlsCiphers;
  }

  public void setTlsCiphers(Set<String> tlsCiphers) {
    this.tlsCiphers = tlsCiphers;
  }

  public Set<String> getTlsProtocols() {
    return tlsProtocols;
  }

  public void setTlsProtocols(Set<String> tlsProtocols) {
    this.tlsProtocols = tlsProtocols;
  }

  public String getTlsTrustStorePassword() {
    return tlsTrustStorePassword;
  }

  public void setTlsTrustStorePassword(String tlsTrustStorePassword) {
    this.tlsTrustStorePassword = tlsTrustStorePassword;
  }

  public String getTlsTrustStorePath() {
    return tlsTrustStorePath;
  }

  public void setTlsTrustStorePath(String tlsTrustStorePath) {
    this.tlsTrustStorePath = tlsTrustStorePath;
  }

  public String getTlsTrustStoreType() {
    return tlsTrustStoreType;
  }

  public void setTlsTrustStoreType(String tlsTrustStoreType) {
    this.tlsTrustStoreType = tlsTrustStoreType;
  }

  public boolean isUseKeyStoreTls() {
    return useKeyStoreTls;
  }

  public void setUseKeyStoreTls(boolean useKeyStoreTls) {
    this.useKeyStoreTls = useKeyStoreTls;
  }

  public boolean isAllowTlsInsecureConnection() {
    return allowTlsInsecureConnection;
  }

  public void setAllowTlsInsecureConnection(boolean allowTlsInsecureConnection) {
    this.allowTlsInsecureConnection = allowTlsInsecureConnection;
  }

  public boolean isEnableTlsHostnameVerification() {
    return enableTlsHostnameVerification;
  }

  public void setEnableTlsHostnameVerification(boolean enableTlsHostnameVerification) {
    this.enableTlsHostnameVerification = enableTlsHostnameVerification;
  }

  public String getTlsAuthCertFilePath() {
    return tlsAuthCertFilePath;
  }

  public void setTlsAuthCertFilePath(String tlsAuthCertFilePath) {
    this.tlsAuthCertFilePath = tlsAuthCertFilePath;
  }

  public String getTlsAuthKeyFilePath() {
    return tlsAuthKeyFilePath;
  }

  public void setTlsAuthKeyFilePath(String tlsAuthKeyFilePath) {
    this.tlsAuthKeyFilePath = tlsAuthKeyFilePath;
  }

  public String getTokenAuthValue() {
    return tokenAuthValue;
  }

  public void setTokenAuthValue(String tokenAuthValue) {
    this.tokenAuthValue = tokenAuthValue;
  }

  public Integer getDeleteType() {
    return deleteType;
  }

  public void setDeleteType(Integer deleteType) {
    this.deleteType = deleteType;
  }

  //pulsar config end
}
