package com.example.transactionmsg.daohelper;

import com.example.transactionmsg.Config;
import com.example.transactionmsg.DBDataSource;
import com.example.transactionmsg.TransactionMsgClient;
import com.example.transactionmsg.common.DAOHelper;
import com.example.transactionmsg.constant.ClientInitException;
import java.sql.Connection;
import java.util.List;
import org.apache.pulsar.client.api.PulsarClientException;

public class DaoHelperTransactionMsgClient extends TransactionMsgClient {

  private DAOHelper dao;

  public DaoHelperTransactionMsgClient(DAOHelper dao, List<DBDataSource> dbDataSources, List<String> topicLists)
      throws PulsarClientException, ClientInitException {
    this(dao, (String) null, dbDataSources, topicLists, new Config());
  }

  public DaoHelperTransactionMsgClient(DAOHelper dao, String mqAddr, List<DBDataSource> dbDataSources,
      List<String> topicLists) throws PulsarClientException, ClientInitException {
    this(dao, mqAddr, dbDataSources, topicLists, new Config());
  }

  public DaoHelperTransactionMsgClient(DAOHelper dao, String mqAddr, List<DBDataSource> dbDataSources,
      List<String> topicLists, Config config) throws PulsarClientException, ClientInitException {
    super(null, mqAddr, dbDataSources, topicLists, config);
    this.dao = dao;
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
      Connection con = this.dao.getConnHelper();
      id = super.sendMsg(con, content, topic, tag, delay);
      return id;
    } catch (Exception e) {
      throw e;
    }
  }

  public DAOHelper getDao() {
    return this.dao;
  }

  public void setDao(DAOHelper dao) {
    this.dao = dao;
  }

}
