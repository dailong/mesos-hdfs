package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.config.NodeConfig;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.util.Arrays;
import java.util.List;

/**
 * JournalNode.
 */
public class JournalNode extends HdfsNode {
  private final Log log = LogFactory.getLog(JournalNode.class);

  public JournalNode(LiveState liveState, IPersistentStateStore persistentStore, HdfsFrameworkConfig config) {
    super(liveState, persistentStore, config, HDFSConstants.JOURNAL_NODE_ID);
  }

  public boolean evaluate(Offer offer) {
    boolean accept = false;

    NodeConfig journalNodeConfig = config.getNodeConfig(HDFSConstants.JOURNAL_NODE_ID);
    if (offerNotEnoughResources(offer, journalNodeConfig.getCpus(), journalNodeConfig.getMaxHeap())) {
      log.info("Offer does not have enough resources");
    } else {
      List<String> deadJournalNodes = persistenceStore.getDeadJournalNodes();

      log.info(deadJournalNodes);

      if (deadJournalNodes.isEmpty()) {
        if (persistenceStore.getJournalNodes().size() == config.getJournalNodeCount()) {
          log.info(String.format("Already running %s journalnodes", config.getJournalNodeCount()));
        } else if (persistenceStore.journalNodeRunningOnSlave(offer.getHostname())) {
          log.info(String.format("Already running journalnode on %s", offer.getHostname()));
        } else if (persistenceStore.dataNodeRunningOnSlave(offer.getHostname())) {
          log.info(String.format("Cannot colocate journalnode and datanode on %s",
            offer.getHostname()));
        } else {
          accept = true;
        }
      } else if (deadJournalNodes.contains(offer.getHostname())) {
        accept = true;
      }
    }

    return accept;
  }

  protected String getExecutorName() {
    return HDFSConstants.NODE_EXECUTOR_ID;
  }

  protected List<String> getTaskTypes() {
    return Arrays.asList(HDFSConstants.JOURNAL_NODE_ID);
  }
}
