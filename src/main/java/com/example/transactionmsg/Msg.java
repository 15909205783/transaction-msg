package com.example.transactionmsg;

import com.example.transactionmsg.Util.DB;

public class Msg {
  private Long id;
  private String topic;
  private DB db;
  private int haveDealedTimes;
  private long createTime;
  private long nextExpireTime;

  public Msg(Long id, DB db, String topic) {
    this.id = id;
    this.topic = topic;
    this.db = db;
    this.haveDealedTimes = 0;
    this.createTime = System.currentTimeMillis();
  }

  public Long getId() {
    return this.id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getTopic() {
    return this.topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public DB getDb() {
    return this.db;
  }

  public void setDb(DB db) {
    this.db = db;
  }

  public int getHaveDealedTimes() {
    return this.haveDealedTimes;
  }

  public void setHaveDealedTimes(int haveDealedTimes) {
    this.haveDealedTimes = haveDealedTimes;
  }

  public long getCreateTime() {
    return this.createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public long getNextExpireTime() {
    return this.nextExpireTime;
  }

  public void setNextExpireTime(long nextExpireTime) {
    this.nextExpireTime = nextExpireTime;
  }

  public String toString() {
    return "Msg[id=" + this.id + ", topic='" + this.topic + '\'' + ", haveDealedTimes=" + this.haveDealedTimes + ", createTime=" + this.createTime + ", nextExpireTime=" + this.nextExpireTime + ']';
  }
}
