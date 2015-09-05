package org.apache.mesos.hdfs.config;

/**
 * NamedPort. 
 * */
public class NamedPort {
  private String name;
  private long port;

  public NamedPort(String name, long port) {
    this.name = name;
    this.port = port;
  }

  public String getName() {
    return name;
  }

  public long getPort() {
    return port;
  }
}
