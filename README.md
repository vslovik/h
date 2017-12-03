https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html

# Hadoop Cluster Setup

Typically one machine in the cluster is designated as the **NameNode**
and another machine as the **ResourceManager**, exclusively. 
These are the masters. 
 
Other services (such as Web App Proxy Server and MapReduce Job History server) are usually 
run either on dedicated hardware or on shared infrastructure, depending upon the load.

The rest of the machines in the cluster act as both **DataNode** and **NodeManager**. 
These are the slaves.

#### Configuring Hadoop in Non-Secure Mode

Hadoop’s Java configuration is driven by two types of important configuration files:

Read-only default configuration - 
- core-default.xml, 
- hdfs-default.xml, 
- yarn-default.xml, 
- mapred-default.xml.

Site-specific configuration - 
- etc/hadoop/core-site.xml,
- etc/hadoop/hdfs-site.xml,
- etc/hadoop/yarn-site.xml,
- etc/hadoop/mapred-site.xml.

To configure the Hadoop cluster you will need to configure the environment 
in which the Hadoop daemons execute as well as the configuration parameters
for the Hadoop daemons.

HDFS daemons are 
- NameNode, 
- SecondaryNameNode, 
- DataNode. 

YARN daemons are 
- ResourceManager, 
- NodeManager, 
- WebAppProxy

#### Configuring Environment of Hadoop Daemons

Administrators should use the 
- etc/hadoop/hadoop-env.sh 
and optionally the 
- etc/hadoop/mapred-env.sh 
and 
- etc/hadoop/yarn-env.sh 
scripts to do site-specific customization of the Hadoop daemons’ process environment.

At the very least, you must specify the JAVA_HOME so that it is correctly defined on each remote node.

#### Operating the Hadoop Cluster

Once all the necessary configuration is complete, distribute the files to the HADOOP_CONF_DIR 
directory on all the machines. This should be the same directory on all machines.

In general, it is recommended that HDFS and YARN run as separate users. In the majority of installations,
HDFS processes execute as ‘hdfs’. YARN is typically using the ‘yarn’ account.

##### Hadoop Startup

To start a Hadoop cluster you will need to start both the HDFS and YARN cluster.

The first time you bring up HDFS, it must be formatted. Format a new distributed filesystem as hdfs:

    $HADOOP_PREFIX/bin/hdfs namenode -format <cluster_name>
    
Start the HDFS NameNode with the following command on the designated node as hdfs:

    [hdfs]$ $HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs start namenode
    
Start a HDFS DataNode with the following command on each designated node as hdfs:

    [hdfs]$ $HADOOP_PREFIX/sbin/hadoop-daemons.sh --config $HADOOP_CONF_DIR --script hdfs start datanode
    
If etc/hadoop/slaves and ssh trusted access is configured (see Single Node Setup), all of the HDFS processes can be started with a utility script. As hdfs:

    [hdfs]$ $HADOOP_PREFIX/sbin/start-dfs.sh

Start the YARN with the following command, run on the designated ResourceManager as yarn:

    [yarn]$ $HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start resourcemanager

Run a script to start a NodeManager on each designated host as yarn:

    [yarn]$ $HADOOP_YARN_HOME/sbin/yarn-daemons.sh --config $HADOOP_CONF_DIR start nodemanager

Start a standalone WebAppProxy server. Run on the WebAppProxy server as yarn. If multiple servers are used with load balancing it should be run on each of them:

    [yarn]$ $HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR start proxyserver

If etc/hadoop/slaves and ssh trusted access is configured (see Single Node Setup), all of the YARN processes can be started with a utility script. As yarn:

    [yarn]$ $HADOOP_PREFIX/sbin/start-yarn.sh

Start the MapReduce JobHistory Server with the following command, run on the designated server as mapred:

    [mapred]$ $HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR start historyserver

##### Hadoop Shutdown

Stop the NameNode with the following command, run on the designated NameNode as hdfs:

    [hdfs]$ $HADOOP_PREFIX/sbin/hadoop-daemon.sh --config $HADOOP_CONF_DIR --script hdfs stop namenode

Run a script to stop a DataNode as hdfs:

    [hdfs]$ $HADOOP_PREFIX/sbin/hadoop-daemons.sh --config $HADOOP_CONF_DIR --script hdfs stop datanode

If etc/hadoop/slaves and ssh trusted access is configured (see Single Node Setup), all of the HDFS processes may be stopped with a utility script. As hdfs:

    [hdfs]$ $HADOOP_PREFIX/sbin/stop-dfs.sh

Stop the ResourceManager with the following command, run on the designated ResourceManager as yarn:

    [yarn]$ $HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR stop resourcemanager

Run a script to stop a NodeManager on a slave as yarn:

    [yarn]$ $HADOOP_YARN_HOME/sbin/yarn-daemons.sh --config $HADOOP_CONF_DIR stop nodemanager

If etc/hadoop/slaves and ssh trusted access is configured (see Single Node Setup), all of the YARN processes can be stopped with a utility script. As yarn:

    [yarn]$ $HADOOP_PREFIX/sbin/stop-yarn.sh

Stop the WebAppProxy server. Run on the WebAppProxy server as yarn. If multiple servers are used with load balancing it should be run on each of them:

    [yarn]$ $HADOOP_YARN_HOME/sbin/yarn-daemon.sh --config $HADOOP_CONF_DIR stop proxyserver

Stop the MapReduce JobHistory Server with the following command, run on the designated server as mapred:

    [mapred]$ $HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh --config $HADOOP_CONF_DIR stop historyserver

#### Web Interfaces

Once the Hadoop cluster is up and running check the web-ui of the components as described below:

- Daemon	Web Interface	Notes
- NameNode	http://nn_host:port/	Default HTTP port is 50070.
- ResourceManager	http://rm_host:port/	Default HTTP port is 8088.
- MapReduce JobHistory Server	http://jhs_host:port/	Default HTTP port is 19888.