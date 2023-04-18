package com.example.transactionmsg.common;

public enum TXMQVersion {
  V1_1_1(111),
  V1_1_2(112),
  V1_1_3(113),
  V1_1_4(114),
  V1_1_5(115),
  V1_1_6(116),
  V1_1_7(117),
  V1_1_8(118);

  public static final TXMQVersion CURRENT_VERSION = V1_1_8;
  public static final String TX_CLIENT_VERSION_PROPERTIES = "zzmq.transaction.client.version";
  private int versionKey;

  private TXMQVersion(int versionKey) {
    this.versionKey = versionKey;
  }

  public int getVersionKey() {
    return this.versionKey;
  }

  public String getVersion() {
    return this.name();
  }

  public static void setCurrentVersionProp() {
    System.setProperty("zzmq.transaction.client.version", String.valueOf(CURRENT_VERSION.getVersionKey()));
  }
}
