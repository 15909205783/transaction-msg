package com.example.transactionmsg;

import java.util.Date;
import java.util.StringJoiner;

public class MsgInfo {
  private Long id;
  private String content;
  private String topic;
  private String tag;
  private Date create_time;
  private int delay;

  public MsgInfo() {
  }

  public Long getId() {
    return this.id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getContent() {
    return this.content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public String getTopic() {
    return this.topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getTag() {
    return this.tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  public Date getCreate_time() {
    return this.create_time;
  }

  public void setCreate_time(Date create_time) {
    this.create_time = create_time;
  }

  public void setDelay(int delay) {
    this.delay = delay;
  }

  public int getDelay() {
    return this.delay;
  }


  public String toString() {
    return (new StringJoiner(", ", MsgInfo.class.getSimpleName() + "[", "]")).add("id=" + this.id).add("content='" + this.content + "'").add("topic='" + this.topic + "'").add("tag='" + this.tag + "'").add("create_time=" + this.create_time).add("delay=" + this.delay).toString();
  }
}
