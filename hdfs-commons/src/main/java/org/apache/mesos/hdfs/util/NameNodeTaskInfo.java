package org.apache.mesos.hdfs.util;

import java.io.Serializable;

/**
 *
 * @author jzajic
 */
public class NameNodeTaskInfo implements Serializable
{

  String hostname;

  String nameTaskId;
  String zkfcTaskId;

  String nameTaskName;
  String zkfcTaskName;

  public NameNodeTaskInfo(String hostname)
  {
    this.hostname = hostname;
  }

  public String getHostname()
  {
    return hostname;
  }

  public void setHostname(String hostname)
  {
    this.hostname = hostname;
  }

  public String getNameTaskId()
  {
    return nameTaskId;
  }

  public void setNameTaskId(String nameTaskId)
  {
    this.nameTaskId = nameTaskId;
  }

  public String getZkfcTaskId()
  {
    return zkfcTaskId;
  }

  public void setZkfcTaskId(String zkfcTaskId)
  {
    this.zkfcTaskId = zkfcTaskId;
  }

  public String getNameTaskName()
  {
    return nameTaskName;
  }

  public void setNameTaskName(String nameTaskName)
  {
    this.nameTaskName = nameTaskName;
  }

  public String getZkfcTaskName()
  {
    return zkfcTaskName;
  }

  public void setZkfcTaskName(String zkfcTaskName)
  {
    this.zkfcTaskName = zkfcTaskName;
  }

  public boolean isDead()
  {
    return getNameTaskId() == null && getZkfcTaskId() == null;
  }

  public NameNodeTaskInfo join(NameNodeTaskInfo otherInstance)
  {
    NameNodeTaskInfo info = new NameNodeTaskInfo(hostname);
    if (zkfcTaskId != null)
    {
      info.setZkfcTaskId(zkfcTaskId);
      info.setZkfcTaskName(zkfcTaskName);
    }
    if (nameTaskId != null)
    {
      info.setNameTaskId(nameTaskId);
      info.setNameTaskName(nameTaskName);
    }

    if (otherInstance != null)
    {
      if (otherInstance.getZkfcTaskId() != null)
      {
        info.setZkfcTaskId(otherInstance.getZkfcTaskId());
        info.setZkfcTaskName(otherInstance.getZkfcTaskName());
      }

      if (otherInstance.getNameTaskId() != null)
      {
        info.setNameTaskId(otherInstance.getNameTaskId());
        info.setNameTaskName(otherInstance.getNameTaskName());
      }
    }

    return info;
  }

}
