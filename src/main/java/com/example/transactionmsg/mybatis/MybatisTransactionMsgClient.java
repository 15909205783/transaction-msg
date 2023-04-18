package com.example.transactionmsg.mybatis;

import com.example.transactionmsg.Config;
import com.example.transactionmsg.DBDataSource;
import com.example.transactionmsg.TransactionMsgClient;
import com.example.transactionmsg.constant.ClientInitException;
import java.sql.Connection;
import java.util.List;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.mybatis.spring.SqlSessionTemplate;
import org.redisson.api.RedissonClient;

public class MybatisTransactionMsgClient extends TransactionMsgClient {

  private SqlSessionTemplate sessionTemplate;
  private SqlSessionFactory sqlSessionFactory;
  private RedissonClient redissonClient;

  public MybatisTransactionMsgClient(SqlSessionTemplate sessionTemplate, RedissonClient redissonClient, String mqAddr,
      List<DBDataSource> dbDataSources, List<String> topicLists,Config config) throws PulsarClientException, ClientInitException {
    super(redissonClient, mqAddr, dbDataSources, topicLists, config);
    this.sessionTemplate = sessionTemplate;
  }

  public MybatisTransactionMsgClient(SqlSessionFactory sqlSessionFactory, RedissonClient redissonClient, String mqAddr,
      List<DBDataSource> dbDataSources, List<String> topicLists, Config config)
      throws PulsarClientException, ClientInitException {
    super(redissonClient, mqAddr, dbDataSources, topicLists, config);
    this.sqlSessionFactory = sqlSessionFactory;

    try {
      this.sessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
    } catch (Exception var7) {
      log.error("get sqlSessionFactory fail", var7);
    }

  }

  public MybatisTransactionMsgClient(SqlSessionFactory sqlSessionFactory, RedissonClient redissonClient, String mqAddr,
      List<DBDataSource> dbDataSources, List<String> topicLists) throws PulsarClientException, ClientInitException {
    this(sqlSessionFactory, redissonClient, mqAddr, dbDataSources, topicLists, new Config());
  }

  public MybatisTransactionMsgClient(SqlSessionFactory sqlSessionFactory, RedissonClient redissonClient,
      List<DBDataSource> dbDataSources, List<String> topicLists) throws PulsarClientException, ClientInitException {
    this(sqlSessionFactory, redissonClient, (String) null, dbDataSources, topicLists, new Config());
  }

  public Long sendMsg(String content, String topic) throws Exception {
    return this.sendMsg(content, topic, (String) null, 0);
  }

  public Long sendMsg(String content, String topic, String tag) throws Exception {
    return this.sendMsg(content, topic, tag, 0);
  }

  public Long sendMsg(String content, String topic, String tag, int delay) throws Exception {
    Long id = null;

    try {
      Connection con = this.sessionTemplate.getConnection();
      id = super.sendMsg(con, content, topic, tag, delay);
      return id;
    } catch (Exception var7) {
      throw new RuntimeException(var7);
    }
  }

  public SqlSessionTemplate getSessionTemplate() {
    return this.sessionTemplate;
  }

  public void setSessionTemplate(SqlSessionTemplate sessionTemplate) {
    this.sessionTemplate = sessionTemplate;
  }

  public SqlSessionFactory getSqlSessionFactory() {
    return this.sqlSessionFactory;
  }

  public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
    this.sqlSessionFactory = sqlSessionFactory;
  }

  public void setHistoryMsgStoreTime(int historyMsgStoreTime) {
    this.getConfig().setHistoryMsgStoreTime(historyMsgStoreTime);
  }

  public int getHistoryMsgStoreTime() {
    return this.getConfig().getHistoryMsgStoreTime();
  }
}
