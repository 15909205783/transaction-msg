package com.example.transactionmsg.common;

import java.sql.Connection;

public class DAOHelper {

  private Connection connection;

  public Connection getConnection() {
    return connection;
  }

  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  public Connection getConnHelper(){

   return this.connection;
  }

}
