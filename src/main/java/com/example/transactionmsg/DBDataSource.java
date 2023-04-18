package com.example.transactionmsg;

public class DBDataSource {
  private String url;
  private String username;
  private String password;

  public DBDataSource() {
  }

  public DBDataSource(String url, String username, String password) {
    this.url = url;
    this.username = username;
    this.password = password;
  }

  public String getUrl() {
    return this.url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUsername() {
    return this.username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return this.password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String toString() {
    return "DBDataSource [url=" + this.url + ", username=" + this.username + ", password=" + this.password + "]";
  }
}
