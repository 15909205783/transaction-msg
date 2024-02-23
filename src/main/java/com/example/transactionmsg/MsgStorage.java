package com.example.transactionmsg;

import com.example.transactionmsg.Util.DB;
import com.example.transactionmsg.common.SystemEnvType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import javax.sql.DataSource;
import org.apache.avro.message.MessageDecoder;
import org.apache.commons.dbcp.BasicDataSource;
//import org.apache.rocketmq.common.message.MessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsgStorage {

  private static String tableName = "mq_messages";
  private static String insertSQL = "insert into %s(content,topic,tag,delay,create_time) values(?,?,?,?,?) ";
  private static String deleteMsgByIDSQL = "DELETE FROM %s WHERE ID ";
  private static String logicDeleteMsgByIDSQL = "UPDATE %s SET deleted = 1 WHERE ID ";

  private static String selectByIdSQL = "select id,content,topic,tag,create_time,delay from %s where id=? ";
  private static String selectWaitingMsgSQL = "select id,content,topic,tag,create_time,delay from %s where create_time <= ? order by id limit ?";
  private static String selectWaitingMsgWithTopicsSQL = "select id,content,topic,tag,create_time,delay from %s where create_time <= ? and topic in ";
  private static String selectMsgIDSQL = "SELECT ID FROM %s WHERE create_time <= ? LIMIT ?";
  private static String selectTableCountSQL = "SELECT count(1) FROM %s";
  private static String checkAddedColumnSQL = "SELECT count(column_name) FROM information_schema.columns WHERE table_schema = DATABASE() and table_name = '%s' AND column_name = ?";
  private static final String delayColumnName = "delay";
  private static final String propertiesColumnName = "properties";
  private static final String driverClass = "com.mysql.jdbc.Driver";
  private static final Logger log = LoggerFactory.getLogger(MsgStorage.class);
  private List<DBDataSource> dbDataSources;
  private HashMap<String, DataSource> dataSourcesMap;
  private HashMap<DB, String> dbUrlMapping;
  private Config config;
  private List<String> topicLists;

  public MsgStorage(List<DBDataSource> dbDataSources, List<String> topicLists) {
    this.dbDataSources = dbDataSources;
    this.dataSourcesMap = new HashMap();
    this.dbUrlMapping = new HashMap();
    this.topicLists = topicLists;
    log.info("MsgStorage topicLists {}", topicLists);
  }

  public void init(Config config) throws Exception {
    log.info("[ARCH_TXMQ_INIT] start init MsgStorage db Size {} msg Store {} day", this.dbDataSources.size(),
        config.getHistoryMsgStoreTime());
    this.config = config;
    if (Objects.equals(config.getSandboxMsgTableName(), config.getMsgTableName())) {
      throw new RuntimeException("请不要把test环境与线上环境配置相同的事物消息表名, 会有数据错乱冲突的风险");
    } else {
      if (SystemEnvUtil.getSysEnv() != SystemEnvType.TEST) {
        tableName = config.getMsgTableName();
      } else {
        tableName = config.getSandboxMsgTableName();
      }

      this.initSql();
      Iterator iterator = this.dbDataSources.iterator();

      while (iterator.hasNext()) {
        DBDataSource dbSrc = (DBDataSource) iterator.next();
        BasicDataSource result = new BasicDataSource();
        result.setDriverClassName("com.mysql.cj.jdbc.Driver");
        result.setUrl(dbSrc.getUrl());
        result.setUsername(dbSrc.getUsername());
        result.setPassword(dbSrc.getPassword());
        result.setInitialSize(this.config.getMinIdleConnectionNum());
        result.setMinIdle(this.config.getMinIdleConnectionNum());
        result.setMaxWait(this.config.getMaxWaitTime());
        result.setMaxActive(this.config.getMaxActiveConnectionNum());
        result.setTestWhileIdle(true);
        result.setValidationQuery("SELECT 1 FROM DUAL;");
        this.checkAddedColumnExists(result, "delay");
        String jdbcUrl = dbSrc.getUrl();
        this.dataSourcesMap.put(jdbcUrl, result);
        this.dbUrlMapping.put(Util.getDataBase(jdbcUrl), jdbcUrl);
      }

      log.info("[ARCH_TXMQ_INIT] init MsgStorage success");
    }
  }

  public void initSql() {
    insertSQL = String.format(insertSQL, tableName);
    log.info("insertSQL = {}", insertSQL);
    selectByIdSQL = String.format(selectByIdSQL, tableName);
    log.info("selectByIdSQL = {}", selectByIdSQL);
    deleteMsgByIDSQL = String.format(deleteMsgByIDSQL, tableName);
    log.info("deleteMsgByIDSQL = {}", deleteMsgByIDSQL);
    logicDeleteMsgByIDSQL = String.format(logicDeleteMsgByIDSQL, tableName);
    log.info("logicDeleteMsgByIDSQL = {}", logicDeleteMsgByIDSQL);
    selectWaitingMsgSQL = String.format(selectWaitingMsgSQL, tableName);
    selectWaitingMsgWithTopicsSQL = String.format(selectWaitingMsgWithTopicsSQL, tableName);
    checkAddedColumnSQL = String.format(checkAddedColumnSQL, tableName);
    selectMsgIDSQL = String.format(selectMsgIDSQL, tableName);
    selectTableCountSQL = String.format(selectTableCountSQL, tableName);
  }

  private void checkAddedColumnExists(BasicDataSource dataSrc, String columnName) throws Exception {
    Connection con = null;
    PreparedStatement psmt = null;
    ResultSet rs = null;

    try {
      con = dataSrc.getConnection();
      psmt = con.prepareStatement(checkAddedColumnSQL);
      psmt.setString(1, columnName);
      rs = psmt.executeQuery();
      boolean delayColumnExists = false;
      if (rs.next()) {
        delayColumnExists = rs.getLong(1) > 0L;
      }

      if (!delayColumnExists) {
        throw new Exception(
            "TransactionMsgClient has been upgraded, its accessing db's table " + tableName + " should add a column \""
                + columnName + "\", please contact with dba to alter the table");
      }
    } catch (SQLException e) {
      throw e;
    } finally {
      closeResultSet(rs);
      closePreparedStatement(psmt);
      if (con != null) {
        con.close();
      }

    }

  }

  public void close() {
    log.info("start close MsgStorage");
    Iterator it = this.dataSourcesMap.entrySet().iterator();

    while (it.hasNext()) {
      Entry entry = (Entry) it.next();
      BasicDataSource dataSrc = (BasicDataSource) entry.getValue();

      try {
        dataSrc.close();
      } catch (SQLException e) {
        log.error("dataSrc={} close fail ", dataSrc.getUrl(), e);
      }
    }

  }

  public static Entry<Long, DB> insertMsg(Connection con, String content, String topic, String tag, int delay) throws SQLException {
    PreparedStatement psmt = null;
    ResultSet results = null;

    SimpleEntry idUrlPair;
    try {


      psmt = con.prepareStatement(insertSQL, 1);
      DatabaseMetaData metaData = con.getMetaData();
      String url = metaData.getURL();
      psmt.setString(1, content);
      psmt.setString(2, topic);
      psmt.setString(3, tag);
      psmt.setInt(4, delay);

      psmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
      psmt.executeUpdate();
      results = psmt.getGeneratedKeys();
      Long id = null;
      if (results.next()) {
        id = results.getLong(1);
      }

      DB db = Util.getDataBase(url);
      if (db.getDataBase() == null || db.getDataBase().length() == 0) {
        db.setDataBase(con.getCatalog());
      }

      idUrlPair = new SimpleEntry(id, db);
    } catch (SQLException e) {
      throw e;
    } finally {
      closeResultSet(results);
      closePreparedStatement(psmt);
    }

    return idUrlPair;
  }

  public MsgInfo getMsgById(Msg msg) throws SQLException {
    if (msg == null) {
      return null;
    } else {
      Long id = msg.getId();
      DataSource dataSrc = this.getDataSource(msg.getDb());
      Connection con = null;
      PreparedStatement psmt = null;
      ResultSet rs = null;

      try {
        con = dataSrc.getConnection();
        psmt = con.prepareStatement(selectByIdSQL);
        psmt.setLong(1, id);
        rs = psmt.executeQuery();
        MsgInfo msgInfo = null;

        while (rs.next()) {
          msgInfo = new MsgInfo();
          msgInfo.setId(rs.getLong(1));
          msgInfo.setContent(rs.getString(2));
          msgInfo.setTopic(rs.getString(3));
          msgInfo.setTag(rs.getString(4));
          msgInfo.setCreate_time(rs.getTimestamp(5));
          msgInfo.setDelay(rs.getInt(6));
        }

        return msgInfo;
      } catch (SQLException e) {
        throw e;
      } finally {
        closeResultSet(rs);
        closePreparedStatement(psmt);
        if (con != null) {
          con.close();
        }

      }
    }
  }

  public List<MsgInfo> getWaitingMsg(DataSource dataSrc, int pageSize) throws SQLException {
    Connection con = null;
    PreparedStatement psmt = null;
    ResultSet rs = null;

    try {
      con = dataSrc.getConnection();
      String sql = selectWaitingMsgSQL;
      boolean flag = false;
      int i;
      if (this.topicLists != null && !this.topicLists.isEmpty()) {
        StringBuilder sb = new StringBuilder(selectWaitingMsgWithTopicsSQL);
        sb.append(" ( ");

        for (i = 0; i < this.topicLists.size(); ++i) {
          if (i < this.topicLists.size() - 1) {
            sb.append(" ? ,");
          } else {
            sb.append(" ? ");
          }
        }

        sb.append(" ) ");
        sb.append(" order by id limit ? ;");
        flag = true;
        sql = sb.toString();
      }

      psmt = con.prepareStatement(sql);
      psmt.setTimestamp(1, this.config.getScanEndTimestamp());
      if (!flag) {
        psmt.setInt(2, pageSize);
      } else {
        int j = 2;

        for (i = 0; i < this.topicLists.size(); ++j) {
          String topic = this.topicLists.get(i);
          psmt.setString(j, topic);
          ++i;
        }

        psmt.setInt(j, pageSize);
      }

      rs = psmt.executeQuery();
      ArrayList list = new ArrayList(pageSize);

      while (rs.next()) {
        MsgInfo msgInfo = new MsgInfo();
        msgInfo.setId(rs.getLong(1));
        msgInfo.setContent(rs.getString(2));
        msgInfo.setTopic(rs.getString(3));
        msgInfo.setTag(rs.getString(4));
        msgInfo.setCreate_time(rs.getTimestamp(5));
        msgInfo.setDelay(rs.getInt(6));
        list.add(msgInfo);
      }

      return list;
    } catch (SQLException e) {
      throw e;
    } finally {
      closeResultSet(rs);
      closePreparedStatement(psmt);
      if (con != null) {
        con.close();
      }

    }
  }

  public Map<String, Long> selectTableCount() throws SQLException {
    Map<String, Long> result = new HashMap();
    Iterator<Entry<String, DataSource>> iterator = this.dataSourcesMap.entrySet().iterator();

    while (iterator.hasNext()) {
      Entry<String, DataSource> entry = iterator.next();
      DataSource dataSrc = entry.getValue();
      Connection con = null;
      PreparedStatement psmt = null;
      ResultSet rs = null;

      try {
        con = dataSrc.getConnection();
        String sql = selectTableCountSQL;
        psmt = con.prepareStatement(sql);
        rs = psmt.executeQuery();

        while (rs.next()) {
          Long count = Optional.of(rs.getLong(1)).orElse(0L);
          result.put(entry.getKey(), count);
        }
      } catch (SQLException e) {
        throw e;
      } finally {
        closeResultSet(rs);
        closePreparedStatement(psmt);
        if (con != null) {
          con.close();
        }

      }
    }

    return result;
  }

  public List<Long> getHistoryMsgID(DataSource dataSrc, int limit) throws SQLException {
    Connection con = null;
    PreparedStatement psmt = null;
    ResultSet rs = null;

    try {
      con = dataSrc.getConnection();
      String sql = selectMsgIDSQL;
      psmt = con.prepareStatement(sql);
      psmt.setTimestamp(1, this.config.getStoreDayBeforeTimeStamp());
      psmt.setInt(2, limit);
      rs = psmt.executeQuery();
      ArrayList list = new ArrayList(limit);

      while (rs.next()) {
        list.add(rs.getLong(1));
      }

      return list;
    } catch (SQLException e) {
      throw e;
    } finally {
      closeResultSet(rs);
      closePreparedStatement(psmt);
      if (con != null) {
        con.close();
      }

    }
  }

  public int deleteMsgByID(DB db, Long ID) throws SQLException {
    DataSource dataSrc = this.getDataSource(db);
    return this.deleteMsgByIDList(dataSrc, 1, ID);
  }

  public int deleteMsgByID(DataSource dataSrc, Long ID) throws SQLException {
    return this.deleteMsgByIDList(dataSrc, 1, ID);
  }


  public int deleteMsgByIDList(DataSource dataSrc, int limitNum, Long... IDList) throws SQLException {
    if (IDList != null && IDList.length != 0) {
      Connection con = null;
      PreparedStatement psmt = null;

      try {
        StringBuilder sql = new StringBuilder();
        //是物理删除还是逻辑删除
        if (Objects.nonNull(config) && config.getDeleteType() == 2) {
          sql.append(deleteMsgByIDSQL);
        } else {
          sql.append(logicDeleteMsgByIDSQL);
        }
        int result;
        if (IDList.length == 1) {
          sql.append("= ? ");
        } else {
          sql.append("IN (");

          for (result = 0; result < IDList.length - 1; ++result) {
            sql.append("?").append(",");
          }

          sql.append("?");
          sql.append(")");
        }

        sql.append(" LIMIT ?");
        con = dataSrc.getConnection();
        psmt = con.prepareStatement(sql.toString());

        for (result = 0; result < IDList.length; ++result) {
          psmt.setLong(result + 1, IDList[result]);
        }

        psmt.setInt(IDList.length + 1, limitNum);
        result = psmt.executeUpdate();
        return result;
      } catch (SQLException e) {
        throw e;
      } finally {
        closePreparedStatement(psmt);
        if (con != null) {
          con.close();
        }

      }
    } else {
      throw new RuntimeException("transaction msg want delete msg by id but IDList is empty");
    }
  }


  protected HashMap<String, DataSource> getDataSourcesMap() {
    return this.dataSourcesMap;
  }

  protected void setDataSourcesMap(HashMap<String, DataSource> dataSourcesMap) {
    this.dataSourcesMap = dataSourcesMap;
  }

  public static String getTablename() {
    return tableName;
  }

  public DataSource getDataSource(DB db) {
    DataSource dataSource = this.dataSourcesMap.get(db.getUrl());
    if (dataSource == null) {
      String url = this.dbUrlMapping.get(db);
      if (url != null && url.length() > 0) {
        dataSource = this.dataSourcesMap.get(url);
      }
    }

    if (dataSource == null) {
      throw new RuntimeException(
          String.format("expect find datasoure from url but find null, db is %s, dataSourcesMap url is %s", db,
              this.dataSourcesMap.keySet()));
    } else {
      return dataSource;
    }
  }

  public List<DBDataSource> getDbDataSources() {
    return this.dbDataSources;
  }

  public void setDbDataSources(List<DBDataSource> dbDataSources) {
    this.dbDataSources = dbDataSources;
  }

  public static void closeResultSet(ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        log.error("close Connection ResultSet error {} ", rs, e);
      }
    }

  }

  public static void closePreparedStatement(PreparedStatement psmt) {
    if (psmt != null) {
      try {
        psmt.close();
      } catch (SQLException e) {
        log.error("close Connection PreparedStatement {} error ", psmt, e);
      }
    }

  }

  public boolean isInTopicLists(String topic) {
    if (topic == null) {
      return false;
    } else {
      for (int i = 0; i < this.topicLists.size(); ++i) {
        String retryTopic = this.topicLists.get(i);
        if (retryTopic != null && retryTopic.equals(topic)) {
          return true;
        }
      }

      return false;
    }
  }

  String getTopicListUniqueKey() {
    if (this.topicLists != null && !this.topicLists.isEmpty()) {
      TreeSet<String> sortedSet = new TreeSet(this.topicLists);
      return Util.encrypt(sortedSet.toString());
    } else {
      return "";
    }
  }
}
