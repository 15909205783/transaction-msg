package com.example.transactionmsg;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Util {
  private static final Logger log = LoggerFactory.getLogger(Util.class);
  private static String defaultEnvPath = "/opt/system.env";
  public static final String SYS_ENV = "sys-env";
  public static final String testServer = "testserver";
  public static final Pattern PATTERN = Pattern.compile("[a-zA-Z0-9_-]");
  public static final Pattern JDBC_WITH_PARAMS = Pattern.compile("^jdbc:mysql://(.*)/(.*)\\?.*$");
  public static final Pattern JDBC_WITHOUT_PARAMS = Pattern.compile("^jdbc:mysql://(.*)/(.*)$");

  public Util() {
  }

  public static String getServerSysEnv() {
    Properties properties = new Properties();

    try {
      InputStream in = new BufferedInputStream(new FileInputStream(defaultEnvPath));
      properties.load(in);
    } catch (IOException var2) {
      log.info("load default /opt/system.env fail {}", var2.getMessage());
    }

    String sysEnv = properties.getProperty("sys-env");
    if (sysEnv == null || sysEnv.isEmpty()) {
      sysEnv = System.getenv("sys-env");
    }

    return sysEnv;
  }

  public static boolean isTestEnv() {
    String sysEnv = getServerSysEnv();
    return sysEnv != null && sysEnv.equals("testserver");
  }

  public static Util.ServerType getServerType() {
    String sysEnv = getServerSysEnv();
    Util.ServerType serverType = Util.ServerType.getType(sysEnv);
    return serverType;
  }

  public static String parseJDBCUrl(String jdbcUrl) {
    if (jdbcUrl != null && !jdbcUrl.isEmpty()) {
      String[] data = jdbcUrl.split("\\?");
      return data != null && data.length != 0 ? data[0] : null;
    } else {
      return null;
    }
  }

  public static Util.DB getDataBase(String jdbcUrl) {
    if (jdbcUrl != null && !jdbcUrl.isEmpty()) {
      Util.DB db = new Util.DB();
      db.setUrl(jdbcUrl);
      Matcher matcher = null;
      if (!jdbcUrl.contains("?")) {
        matcher = JDBC_WITHOUT_PARAMS.matcher(jdbcUrl);
      } else {
        matcher = JDBC_WITH_PARAMS.matcher(jdbcUrl);
      }

      if (matcher.matches()) {
        String hostname = matcher.group(1);
        int portIndex = hostname.lastIndexOf(":");
        String port = "80";
        if (portIndex > 0) {
          port = hostname.substring(portIndex + 1);
          hostname = hostname.substring(0, portIndex);
        }

        String dabaBase = matcher.group(2);
        db.setHostname(hostname);
        db.setPort(port);
        db.setDataBase(dabaBase);
      }

      return db;
    } else {
      return null;
    }
  }

  public static String encrypt(String dataStr) {
    try {
      MessageDigest m = MessageDigest.getInstance("MD5");
      m.update(dataStr.getBytes("UTF8"));
      byte[] encryContext = m.digest();
      StringBuffer buf = new StringBuffer();

      for(int offset = 0; offset < encryContext.length; ++offset) {
        int i = encryContext[offset];
        if (i < 0) {
          i += 256;
        }

        if (i < 16) {
          buf.append("0");
        }

        buf.append(Integer.toHexString(i));
      }

      log.info("TransactionMsgClient MD5 encrypt key {} result {} ", dataStr, buf);
      return buf.toString();
    } catch (UnsupportedEncodingException | NoSuchAlgorithmException var6) {
      log.error("TransactionMsgClient encrypt", var6);
      return "";
    }
  }

  public static void main(String[] args) {
    System.out.println(getServerSysEnv());
    String str = "jdbc:mysql://test19685.db.58dns.org:23666/zzredis";
    System.out.println(parseJDBCUrl(str));
    Util.DB db = getDataBase(str);
    System.out.println(db);
    System.out.println(db.buildKey());
    System.out.println(getServerType());
    System.out.println(Util.ServerType.getType("testserver"));
  }

  static enum ServerType {
    Online("online"),
    Sandbox("sandbox"),
    TestServer("testserver"),
    NONE("NONE");

    private String type;

    private ServerType(String type) {
      this.type = type;
    }

    public String getType() {
      return this.type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public static Util.ServerType getType(String typeName) {
      Util.ServerType[] types = values();
      Util.ServerType[] var2 = types;
      int var3 = types.length;

      for(int var4 = 0; var4 < var3; ++var4) {
        Util.ServerType type = var2[var4];
        if (type.getType().equals(typeName)) {
          return type;
        }
      }

      return NONE;
    }
  }

  static class DB {
    String hostname;
    String port;
    String dataBase;
    String url;

    DB() {
    }

    public String getUrl() {
      return this.url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public String getHostname() {
      return this.hostname;
    }

    public void setHostname(String hostname) {
      this.hostname = hostname;
    }

    public String getPort() {
      return this.port;
    }

    public void setPort(String port) {
      this.port = port;
    }

    public String getDataBase() {
      return this.dataBase;
    }

    public void setDataBase(String dataBase) {
      this.dataBase = dataBase;
    }

    public String buildKey() {
      String str = this.hostname + "_" + this.port + "_" + this.getDataBase();
      str = str.replace(".", "");
      return str;
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof Util.DB)) {
        return false;
      } else {
        Util.DB db = (Util.DB)o;
        return Objects.equals(this.getHostname(), db.getHostname()) && Objects.equals(this.getPort(), db.getPort()) && Objects.equals(this.getDataBase(), db.getDataBase());
      }
    }

    public int hashCode() {
      return Objects.hash(new Object[]{this.getHostname(), this.getPort(), this.getDataBase()});
    }

    public String toString() {
      return (new StringJoiner(", ", Util.DB.class.getSimpleName() + "[", "]")).add("hostname='" + this.hostname + "'").add("port='" + this.port + "'").add("dataBase='" + this.dataBase + "'").add("url='" + this.url + "'").toString();
    }
  }
}
