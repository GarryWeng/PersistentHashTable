package com.companyname.project.zkstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.companyname.project.api.PersistentHashTable;
import com.companyname.project.conf.Configuration;
import com.companyname.project.util.ZKUtil;
import com.companyname.project.proto.*;
import com.companyname.project.proto.Metadata.KeyValueListProto;
import com.companyname.project.proto.Metadata.KeyValueProto;

public class PersistentHashTableOnZk extends PersistentHashTable {

  protected static final Log LOG = LogFactory
      .getLog(PersistentHashTableOnZk.class);

  private static final int DEFAULT_HASHTABLE_BUCKETS_NUM = 60;

  protected static final String DEFAULT_DIR_NAME = "persistent-hashtable";
  protected static final String DEFAULT_BUCKET_PREFIX = "hashtable-bucket";
  protected static final String DEFAULT_NODE_LABEL_LATEST_VERSION =
      "zk-latest-version";

  private String zkWorkingPath;

  private String defaultzkLatestPath;
  private String zkLatestPath;
  private String zkNewPath;

  private String latestVersion;
  private String newVersion;

  private List<ACL> zkAcl;
  private List<ZKUtil.ZKAuthInfo> zkAuths;
  private ZooKeeper zkClient;
  private String zkHost;
  private int zkSessionTimeout;
  private int numRetries;
  private long zkResyncWaitTime;
  private long zkRetryInterval;
  
  /*
   * activeZkClient is not used to do actual operations, it is only used to
   * verify client session for watched events and it gets activated into
   * zkClient on connection event.
   */
  private ZooKeeper activeZkClient;

  private Hashtable<String, String> hashtable = new Hashtable<String, String>();

  private boolean initialized = false;

  @Override
  public void put(String key, String value) {
    hashtable.put(key, value);
    store();
  }

  @Override
  public String get(String key) {
    return hashtable.get(key);
  }
  
  @Override
  public void init(Configuration conf) throws Exception {
    zkHost = conf.get(Configuration.ZK_ADDRESS);
    if (zkHost == null) {
      throw new IOException("No server address specified for "
          + "zookeeper state store for node labels recovery. "
          + Configuration.ZK_ADDRESS + " is not configured.");
    }

    zkSessionTimeout =
        conf.getInt(Configuration.ZK_TIMEOUT_MS,
            Configuration.DEFAULT_ZK_TIMEOUT_MS);
    zkAcl = getZKAcls(conf);
    zkAuths = getZKAuths(conf);

    numRetries =
        conf.getInt(Configuration.ZK_NUM_RETRIES,
            Configuration.DEFAULT_ZK_NUM_RETRIES);

    zkRetryInterval =
        conf.getLong(Configuration.ZK_RETRY_INTERVAL_MS,
            Configuration.DEFAULT_ZK_RETRY_INTERVAL_MS);

    zkResyncWaitTime = zkRetryInterval * numRetries;

    zkWorkingPath =
        conf.get(Configuration.ZK_STORE_ROOT_DIR,
            getDefaultZKNodeLabelsRootDir());
    defaultzkLatestPath =
        conf.get(Configuration.ZK_HASHTABLE_STORE_DIR,
            getDefaultZKStoreDir());

    createConnection();

    if (null == existsPath(zkWorkingPath, null)) {
      createDirWithParents(zkWorkingPath);
    }

    if (null == existsPath(
        getNodePath(zkWorkingPath, DEFAULT_NODE_LABEL_LATEST_VERSION), null)) {
      createDirWithParents(getNodePath(zkWorkingPath,
          DEFAULT_NODE_LABEL_LATEST_VERSION));
    }

  }
  
  private String getDefaultZKNodeLabelsRootDir() throws IOException {
    return Configuration.DEFAULT_ZK_STATE_STORE_PARENT_PATH + "/"
        + DEFAULT_DIR_NAME;
  }

  private String getDefaultZKStoreDir() throws IOException {
    return Configuration.DEFAULT_ZK_STATE_STORE_PARENT_PATH + "/"
        + DEFAULT_DIR_NAME + "/ZKStore";
  }
  
  private List<ACL> getZKAcls(Configuration conf) throws Exception {
    // Parse authentication from configuration.
    String zkAclConf =
        conf.get(Configuration.ZK_ACL,
            Configuration.DEFAULT_ZK_ACL);
    try {
      zkAclConf = ZKUtil.resolveConfIndirection(zkAclConf);
      return ZKUtil.parseACLs(zkAclConf);
    } catch (Exception e) {
      LOG.error("Couldn't read ACLs based on " + Configuration.ZK_ACL);
      throw e;
    }
  }

  private List<ZKUtil.ZKAuthInfo> getZKAuths(Configuration conf)
      throws Exception {
    String zkAuthConf = conf.get(Configuration.ZK_AUTH);
    try {
      zkAuthConf = ZKUtil.resolveConfIndirection(zkAuthConf);
      if (zkAuthConf != null) {
        return ZKUtil.parseAuth(zkAuthConf);
      } else {
        return Collections.emptyList();
      }
    } catch (Exception e) {
      LOG.error("Couldn't read Auth based on " + Configuration.ZK_AUTH);
      throw e;
    }
  }

  private void store() {

    getNewNodeLabelPath();
    if (!initNodeIdBuckets(zkNewPath)) {
      LOG.error("node id buckets initilize fails");
      return;
    }

    try {

      byte[] data = null;

      Hashtable<String, String> hashtableBuckets[] =
          new Hashtable[DEFAULT_HASHTABLE_BUCKETS_NUM];

      for (Map.Entry<String, String> entry : hashtable.entrySet()) {
        int iteration =
            Math.abs(entry.getKey().hashCode() % DEFAULT_HASHTABLE_BUCKETS_NUM);
        if (hashtableBuckets[iteration] == null) {
          hashtableBuckets[iteration] = new Hashtable<String, String>();
        }
        hashtableBuckets[iteration].put(entry.getKey(), entry.getValue());
      }
      for (int i = 0; i < DEFAULT_HASHTABLE_BUCKETS_NUM; i++) {
        String nodeLabelPath =
            getNodePath(zkNewPath, getPartitionZnodeName(i));

        data =
            hashtableBuckets[i] != null ? getProtoFromHashtable(
                hashtableBuckets[i]).toByteArray() : null;

        setData(nodeLabelPath, data, -1);
      }

      setLatestNodeLabelVersion(newVersion);
      cleanUpOldVersion();
    } catch (Exception e) {
      LOG.error("Execute NodeToLabels update in zookeeper fail "
          + zkLatestPath);
      e.printStackTrace();
    }

  }
  
  private Hashtable<String, String> getHashtableFromProto(
      KeyValueListProto keyValueList) {
    Hashtable<String, String> hashtable = null;
    for (KeyValueProto keyValue : keyValueList.getKeyvalueList()) {
      if (hashtable == null) {
        hashtable = new Hashtable<String, String>();
      }
      hashtable.put(keyValue.getKey(), keyValue.getValue());
    }
    return hashtable;
  }
  
  private KeyValueListProto getProtoFromHashtable(
      Hashtable<String, String> hashtable) {
    if(hashtable == null){
      return null;
    }
    KeyValueListProto keyValueList = null;
    KeyValueListProto.Builder listBuilder = KeyValueListProto.newBuilder();
    
    for ( Map.Entry<String, String> entry : hashtable.entrySet()) {
      KeyValueProto.Builder keyValueBuilder = KeyValueProto.newBuilder();
      keyValueBuilder.setKey(entry.getKey());
      keyValueBuilder.setValue(entry.getValue());
      listBuilder.addKeyvalue(keyValueBuilder.build());
    }
    keyValueList = listBuilder.build();
    return keyValueList;
  }
  
  private Stat setLatestNodeLabelVersion(String version) throws Exception {
    return setData(
        getNodePath(zkWorkingPath, DEFAULT_NODE_LABEL_LATEST_VERSION),
        version.getBytes(), -1);
  }
  
  // Since RM switch or RM crash, need to clean all the unused info, only remain
  // the latest one
  private void cleanUpOldVersion() {
    String nodePath = "";
    try {
      getLatestNodeLabelPath();
      List<String> nodes = this.getChildren(zkWorkingPath, null);
      for (String node : nodes) {
        nodePath = getNodePath(zkWorkingPath, node);
        if (nodePath.startsWith(defaultzkLatestPath)) {
          // This should be the node we cared to delete
          if (!nodePath.equals(zkLatestPath)) {
            delPathWithChildren(nodePath, -1);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("clean up old version fail, nodePath " + nodePath);
      e.printStackTrace();
    }
  }
  
  private void getLatestNodeLabelPath() throws Exception {
    byte[] version =
        getData(zkWorkingPath + "/" + DEFAULT_NODE_LABEL_LATEST_VERSION, null);
    if (version == null) {
      zkLatestPath = defaultzkLatestPath;
    } else {
      latestVersion = new String(version);
      zkLatestPath =
          defaultzkLatestPath + latestVersion;
    }
    LOG.info( "hashtable data path " + zkLatestPath);
  }

  @Override
  public void recover() {
    hashtable=getHashTableFromZK();
    if(hashtable != null){
      initialized = true;
    } else {
      initialized = false;
    }
  }
  
  private String getPartitionZnodeName(int i) {
    return DEFAULT_BUCKET_PREFIX + i;
  }

  public boolean initNodeIdBuckets(String path) {
    try {
      if (null == existsPath(path, null)) {
        createDirWithParents(path);
      }
      List<Op> execOpList = new ArrayList<Op>();
      Op createOp = null;
      for (int i = 0; i < DEFAULT_HASHTABLE_BUCKETS_NUM; i++) {
        String nodeLabelPath = getNodePath(path, getPartitionZnodeName(i));
        createOp = Op.create(nodeLabelPath, null, zkAcl, CreateMode.PERSISTENT);
        execOpList.add(createOp);
      }
      doStoreMulti(execOpList);
      return true;
    } catch (Exception e) {
      LOG.error("initionlize NodeId Buckets fail.");
      e.printStackTrace();
    }
    return false;
  }

  private Hashtable<String, String> getHashTableFromZK() {
    
    Hashtable<String, String> hashtable = new Hashtable<String, String> ();
    try {
      getLatestNodeLabelPath();
      Set<String> nodeToLabelsSet;
      List<String> nodeToLabelsList = getChildren(zkLatestPath, null);
      nodeToLabelsSet = new HashSet<String>(nodeToLabelsList);

      for (String nodeIdString : nodeToLabelsSet) {
        // skip improper node
        int index = getPartitionNumFromZnodeName(nodeIdString);
        if (!nodeIdString.startsWith(DEFAULT_BUCKET_PREFIX) || index == -1) {
          LOG.info("Skip improper node " + nodeIdString + " without prefix "
              + DEFAULT_BUCKET_PREFIX);
          continue;
        }

        String nodeToLabelPath =
            getNodePath(zkLatestPath, nodeIdString);
        byte[] rawData = getData(nodeToLabelPath, null);
        if(rawData != null){
          KeyValueListProto keyValueList = KeyValueListProto.parseFrom(rawData);
          Hashtable<String, String> smallHashtable = getHashtableFromProto(keyValueList);
          hashtable.putAll(smallHashtable);
        }
      }
    } catch (Exception e) {
      LOG.error("recover labels from zookeeper " + zkLatestPath
          + " fails");
    }
    return hashtable;
  }
  
  private void getNewNodeLabelPath() {
    // get a not exist version
    try {
      zkNewPath =
          createSequentialDirWithParents(defaultzkLatestPath);
      if (zkNewPath == null || zkNewPath.isEmpty()) {
        LOG.error("get New NodeLabel Path fail, the new path is empty.");
        return;
      }
      newVersion =
          zkNewPath.substring(defaultzkLatestPath.length());
    } catch (Exception e) {
      LOG.error("get New NodeLabel Path fail. ");
      e.printStackTrace();
    }
  }
  
  String getNodePath(String root, String nodeName) {
    return (root + "/" + nodeName);
  }
  
  private int getPartitionNumFromZnodeName(String znodeName) {
    int result = -1;
    try {
      String numString = znodeName.substring(DEFAULT_BUCKET_PREFIX.length());
      result = Integer.parseInt(numString);
    } catch (NumberFormatException e) {
      LOG.error("fail to get partition num from Znode name");
    }
    return result;
  }

  private synchronized void createConnection() throws IOException,
      InterruptedException {
    closeZkClients();
    for (int retries = 0; retries < numRetries && zkClient == null; retries++) {
      try {
        activeZkClient = getNewZooKeeper();
        zkClient = activeZkClient;
        for (ZKUtil.ZKAuthInfo zkAuth : zkAuths) {
          zkClient.addAuthInfo(zkAuth.getScheme(), zkAuth.getAuth());
        }
      } catch (IOException ioe) {
        // Retry in case of network failures
        LOG.info("Failed to connect to the ZooKeeper on attempt - "
            + (retries + 1));
        ioe.printStackTrace();
      }
    }
    if (zkClient == null) {
      LOG.error("Unable to connect to Zookeeper");
      throw new IOException("Unable to connect to Zookeeper");
    }
    notifyAll();
    LOG.info("Created new ZK connection");
  }

  protected final class ForwardingWatcher implements Watcher {
    private ZooKeeper watchedZkClient;

    public ForwardingWatcher(ZooKeeper client) {
      this.watchedZkClient = client;
    }

    public void process(WatchedEvent event) {
      try {
        processWatchEvent(watchedZkClient, event);
      } catch (Throwable t) {
        LOG.error("Failed to process watcher event " + event);
      }
    }
  }

  // use watch to handle the SyncConnected, Disconnected, Expired event.
  protected synchronized void processWatchEvent(ZooKeeper zk, WatchedEvent event)
      throws Exception {
    // only process watcher event from current ZooKeeper Client session.
    if (zk != activeZkClient) {
      LOG.info("Ignore watcher event type: " + event.getType() + " with state:"
          + event.getState() + " for path:" + event.getPath()
          + " from old session");
      return;
    }

    Event.EventType eventType = event.getType();
    LOG.info("Watcher event type: " + eventType + " with state:"
        + event.getState() + " for path:" + event.getPath() + " for " + this);

    if (eventType == Event.EventType.None) {

      // the connection state has changed
      switch (event.getState()) {
      case SyncConnected:
        LOG.info("ZKRMStateStore Session connected");
        if (zkClient == null) {
          // the SyncConnected must be from the client that sent Disconnected
          zkClient = activeZkClient;
          notifyAll();
          LOG.info("ZKRMStateStore Session restored");
        }
        break;
      case Disconnected:
        LOG.info("ZKRMStateStore Session disconnected");
        zkClient = null;
        break;
      case Expired:
        // the connection got terminated because of session timeout
        // call listener to reconnect
        LOG.info("ZKRMStateStore Session expired");
        createConnection();
        break;
      default:
        LOG.error("Unexpected Zookeeper" + " watch event state: "
            + event.getState());
        break;
      }
    }
  }

  protected synchronized ZooKeeper getNewZooKeeper() throws IOException,
      InterruptedException {
    ZooKeeper zk = new ZooKeeper(zkHost, zkSessionTimeout, null);
    zk.register(new ForwardingWatcher(zk));
    return zk;
  }

  private synchronized void closeZkClients() throws IOException {
    zkClient = null;
    if (activeZkClient != null) {
      try {
        activeZkClient.close();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing ZK", e);
      }
      activeZkClient = null;
    }
  }

  private Stat existsPath(final String path, final Watcher watch)
      throws Exception {
    return new ZKAction<Stat>() {
      @Override
      Stat run() throws KeeperException, InterruptedException {
        return zkClient.exists(path, watch);
      }
    }.runWithRetries();
  }

  private void createDir(final String path) throws Exception {
    new ZKAction<Void>() {
      @Override
      Void run() throws KeeperException, InterruptedException {
        zkClient.create(path, null, zkAcl, CreateMode.PERSISTENT);
        return null;
      }
    }.runWithRetries();
  }

  private String createSequentialDir(final String path) throws Exception {
    return new ZKAction<String>() {
      @Override
      String run() throws KeeperException, InterruptedException {
        return zkClient.create(path, null, zkAcl,
            CreateMode.PERSISTENT_SEQUENTIAL);
      }
    }.runWithRetries();
  }

  private String createSequentialDirWithParents(String path) throws Exception {
    if (path.length() > 0 && path.lastIndexOf("/") > 0) {
      String temp = path.substring(0, path.lastIndexOf("/"));
      createDirWithParents(temp);
      return createSequentialDir(path);
    } else {
      return "";
    }
  }

  private void createDirWithParents(String path) throws Exception {
    if (path.length() > 0 && null == existsPath(path, null)) {
      String temp = path.substring(0, path.lastIndexOf("/"));
      createDirWithParents(temp);
      createDir(path);
    } else {
      return;
    }
  }

  private List<String> getChildren(final String path, final Watcher watch)
      throws Exception {
    return new ZKAction<List<String>>() {
      @Override
      List<String> run() throws KeeperException, InterruptedException {
        return zkClient.getChildren(path, watch);
      }
    }.runWithRetries();
  }

  private byte[] getData(final String path, final Watcher watch)
      throws Exception {
    return new ZKAction<byte[]>() {
      @Override
      public byte[] run() throws KeeperException, InterruptedException {
        return zkClient.getData(path, watch, null);
      }
    }.runWithRetries();
  }

  private Stat setData(final String path, final byte[] data, final int version)
      throws Exception {
    return new ZKAction<Stat>() {
      @Override
      Stat run() throws KeeperException, InterruptedException {
        return zkClient.setData(path, data, version);
      }
    }.runWithRetries();
  }

  private synchronized void doStoreMulti(final List<Op> opList)
      throws Exception {
    new ZKAction<Void>() {
      @Override
      public Void run() throws KeeperException, InterruptedException {
        zkClient.multi(opList);
        return null;
      }
    }.runWithRetries();
  }

  private synchronized void delPath(final String path, final int version)
      throws Exception {
    new ZKAction<Void>() {
      @Override
      public Void run() throws KeeperException, InterruptedException {
        zkClient.delete(path, version);
        return null;
      }
    }.runWithRetries();
  }

  private synchronized void delPathWithChildren(String path, int version)
      throws Exception {
    List<String> children = this.getChildren(path, null);
    for (String child : children) {
      delPathWithChildren(getNodePath(path, child), version);
    }
    delPath(path, version);
  }

  private abstract class ZKAction<T> {
    private boolean hasDeleteNodeOp = false;

    void setHasDeleteNodeOp(boolean hasDeleteOp) {
      this.hasDeleteNodeOp = hasDeleteOp;
    }

    // run() expects synchronization on ZKRMStateStore.this
    abstract T run() throws KeeperException, InterruptedException;

    T runWithCheck() throws Exception {
      long startTime = System.currentTimeMillis();
      synchronized (PersistentHashTableOnZk.this) {
        while (zkClient == null) {
          PersistentHashTableOnZk.this.wait(zkResyncWaitTime);
          if (zkClient != null) {
            break;
          }
          if (System.currentTimeMillis() - startTime > zkResyncWaitTime) {
            throw new IOException("Wait for ZKClient creation timed out");
          }
        }
        return run();
      }
    }

    private boolean shouldRetry(Code code) {
      switch (code) {
      case CONNECTIONLOSS:
      case OPERATIONTIMEOUT:
        return true;
      default:
        break;
      }
      return false;
    }

    private boolean shouldRetryWithNewConnection(Code code) {
      // For fast recovery, we choose to close current connection after
      // SESSIONMOVED occurs. Latest state of a zknode path is ensured by
      // following zk.sync(path) operation.
      switch (code) {
      case SESSIONEXPIRED:
      case SESSIONMOVED:
        return true;
      default:
        break;
      }
      return false;
    }

    T runWithRetries() throws Exception {
      int retry = 0;
      while (true) {
        try {
          return runWithCheck();
        } catch (KeeperException ke) {
          if (ke.code() == Code.NODEEXISTS) {
            LOG.info("znode already exists!");
            return null;
          }
          if (hasDeleteNodeOp && ke.code() == Code.NONODE) {
            LOG.info("znode has already been deleted!");
            return null;
          }

          LOG.info("Exception while executing a ZK operation.", ke);
          retry++;
          if (shouldRetry(ke.code()) && retry < numRetries) {
            LOG.info("Retrying operation on ZK. Retry no. " + retry);
            Thread.sleep(zkRetryInterval);
            continue;
          }
          if (shouldRetryWithNewConnection(ke.code()) && retry < numRetries) {
            LOG.info("Retrying operation on ZK with new Connection. "
                + "Retry no. " + retry);
            Thread.sleep(zkRetryInterval);
            createConnection();
            syncInternal(ke.getPath());
            continue;
          }
          LOG.info("Maxed out ZK retries. Giving up!");
          throw ke;
        }
      }
    }

    class ZKSyncOperationCallback implements AsyncCallback.VoidCallback {
      public void processResult(int rc, String path, Object ctx) {
        if (rc == Code.OK.intValue()) {
          LOG.info("ZooKeeper sync operation succeeded. path: " + path);
        } else {
          LOG.fatal("ZooKeeper sync operation failed. Waiting for session "
              + "timeout. path: " + path);
        }
      }
    }

    private void syncInternal(final String path) throws InterruptedException {
      if (path == null) {
        LOG.error("the sync path is null.");
        return;
      }
      final ZKSyncOperationCallback cb = new ZKSyncOperationCallback();
      final String pathForSync = path;
      try {
        new ZKAction<Void>() {
          @Override
          Void run() throws KeeperException, InterruptedException {
            zkClient.sync(pathForSync, cb, null);
            return null;
          }
        }.runWithRetries();
      } catch (Exception e) {
        LOG.fatal("sync failed.");
      }
    }
  }
}
