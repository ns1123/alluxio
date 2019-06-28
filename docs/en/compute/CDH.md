---
layout: global
title: Integrating CDH Compute with Alluxio
nickname: CDH Compute Frameworks
group: Data Applications
priority: 3
---

This guide describes how to configure Cloudera's Distribution of Hadoop (CDH) compute frameworks to work with Alluxio.

## Prerequisites

You should already have [Cloudera's Distribution](http://www.cloudera.com/) installed.
CDH 5 has been tested and the Cloudera Manager is used for the instructions in the rest of this document.

It is also assumed that Alluxio has been installed on the cluster.

## Running CDH MapReduce

To run CDH MapReduce applications with Alluxio, some additional configuration is required.

### Configuring core-site.xml

You need to add the following properties to `core-site.xml`. The ZooKeeper properties are only required for a cluster
using HA mode. Similarly, embedded properties are only required for an HA cluster using Embedded Journal.

  * `fs.alluxio.impl=alluxio.hadoop.FileSystem`
  * `alluxio.zookeeper.enabled=true`
  * `alluxio.zookeeper.address=zknode1:2181,zknode2:2181,zknode3:2181`
  * `alluxio.master.embedded.journal.addresses=alluxiomaster1:19200,alluxiomaster2:19200,alluxiomaster3:19200`

You can add configuration properties to `core-site.xml` file with the Cloudera Manager. In the "HDFS" component of
the Cloudera Manager, in the "Configuration" tab, parameters can be searched for. The configuration parameter
"Cluster-wide Advanced Configuration Snippet (Safety Valve) for core-site.xml" can be modified to add the
required properties.

The properties can be added as seem below:

![CDHCoreSite]({{ '/img/screenshot_cdh_compute_core_site.png' | relativize_url }})

Then save the configuration, and the Cloudera Manager will notify you that you should restart and deploy
the affected components. Please restart the affected components and deploy the configuration.

### Configuring HADOOP_CLASSPATH

In order for the Alluxio client jar to be available to the MapReduce applications, you must add
the Alluxio Hadoop client jar to the `$HADOOP_CLASSPATH` environment variable in `hadoop-env.sh`.

In the "YARN (MR2 Included)" section of the Cloudera Manager, in the "Configuration" tab, search for the parameter
"Gateway Client Environment Advanced Configuration Snippet (Safety Valve) for hadoop-env.sh". Then add the following
line to the script:

```bash
HADOOP_CLASSPATH=/path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar:${HADOOP_CLASSPATH}
```

It should look something like this:

![CDHHadoopClasspath]({{ '/img/screenshot_cdh_compute_hadoop_classpath.png' | relativize_url }})

After saving the configuration, Cloudera Manager will notify you that the stale configuration files need to be
redeployed and the affected components need to be restarted. Make sure to accept both options and restart the services.
If using Alluxio with HDFS journaling, make sure that you stop Alluxio before rebooting HDFS.

### Security

Since MapReduce runs on YARN, a non-secured Alluxio will need to be configured to allow the `yarn` user to impersonate
other users. To do this, add the below property to `alluxio-site.properties` on Alluxio Masters and Workers and then
restart the Alluxio cluster.

```bash
alluxio.master.security.impersonation.yarn.users=*
```

This is not required if Alluxio and YARN are Kerberized and Secured.

### Running MapReduce Applications

In order for MapReduce applications to be able to read and write files in Alluxio, the Alluxio
client jar must be distributed to all YARN nodes in the cluster and added to the application
classpath.

Below are instructions for the 2 main alternatives for distributing the client jar:

1.**Using the -libjars command line option.**
You can run a job by using the `-libjars` command line option when using `yarn jar ...`,
specifying
`/path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar` as the argument. This
will place the jar in the Hadoop DistributedCache, making it available to all the nodes. For
example, the following command adds the Alluxio client jar to the `-libjars` option:

```bash
$ yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar randomtextwriter -libjars /path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar <OUTPUT URI>
```

2.**Setting the classpath configuration variables**
If the Alluxio client jar is already distributed to all the nodes in the same path, you can add that jar to the
classpath using the `mapreduce.application.classpath` variable.

In the Cloudera Manager, you can find the `mapreduce.application.classpath` variable in the "YARN (MR2 Included)" component, in the "Configuration" tab. For the "MR Application Classpath", add the Alluxio Hadoop client jar as a new entry.

```
/path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar
```

This will be added to the `mapreduce.application.classpath` parameter. It should look something like this:

![CDHMRClasspath]({{ '/img/screenshot_cdh_compute_mr_classpath.png' | relativize_url }})

After you save the configuration, restart the affected components.

### Running Sample MapReduce Application

Here is an example of running a simple MapReduce application. Note that if Alluxio is running in fault tolerant
mode, the URI scheme would need to be `alluxio-ft://` instead of `alluxio://`. In the following example,
replace `MASTER_HOSTNAME` with your actual Alluxio master hostname.

```bash
$ yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar randomtextwriter -Dmapreduce.randomtextwriter.bytespermap=10000000 alluxio://MASTER_HOSTNAME:19998/testing/randomtext/
```

After this job completes, there will be randomly generated text files in the `/testing/randomtext` directory in Alluxio.


## Running CDH HBase

### Add Alluxio Client Jar
We need to make the Alluxio client jar file available to HBase,
because it contains the configured `alluxio.hadoop.FileSystem` class.

It is required put the `alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar` file into the lib directories of HBase.
Please ensure this is done on all the CDH HBase nodes.

```bash
cp /path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar /opt/cloudera/parcels/CDH/jars/ ;
cp /path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar /opt/cloudera/parcels/CDH/lib/hbase ;
cp /path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar /opt/cloudera/parcels/CDH/lib/hbase/lib ;
cp /path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar /opt/cloudera/parcels/CDH/lib/hbase-solr/lib ;
```

Optionally, specify the location of the jar file in the `$HBASE_CLASSPATH` environment variable.
In the "HBase" section of the Cloudera Manager, in the "Configuration" tab, add the following to the sections:

  * HBase Service Environment Advanced Configuration Snippet (Safety Valve)
  * HBase Client Environment Advanced Configuration Snippet (Safety Valve) for hbase-env.sh

```bash
HBASE_CLASSPATH=/path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar:${HBASE_CLASSPATH}
```

It should look something like this:
![CDHHbaseEnv]({{ '/img/screenshot_cdh_hbase_env_classpath.png' | relativize_url }})

Then save the configuration. Do not restart HBase until the next steps are completed.

### Configuring hbase-site.xml

Before restarting HBase running on Alluxio, please create these directories:

```bash
./bin/alluxio fs mkdir /tmp
./bin/alluxio fs chmod 0777 /tmp
./bin/alluxio fs mkdir /hbase
./bin/alluxio fs chmod 0777 /hbase
```

You need to add the following properties to `hbase-site.xml`.

```
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
<property>
  <name>hbase.rootdir</name>
  <value>alluxio://HOSTNAME:PORT/hbase</value>
</property>
```

In the "HBase" section of the Cloudera Manager, in the "Configuration" tab, search for the parameter
"hbase-site.xml". There will be multiple sections which contains "hbase-site.xml".
Please ensure Alluxio required properties are added into both Hbase Service and Client configuration.

- "HBase Service Advanced Configuration Snippet (Safety Valve) for hbase-site.xml"
- "HBase Client Advanced Configuration Snippet (Safety Valve) for hbase-site.xml"

It should look something like this:
![CDHHbaseEnv]({{ '/img/screenshot_cdh_hbase_site.png' | relativize_url }})

Then save the configuration, and the Cloudera Manager will notify you that you should restart the affected
components. Please restart the affected components.

### Add additional Alluxio site properties in HBase 
If there are any Alluxio site properties you want to specify for HBase, add those to `hbase-site.xml`
similarly as setting the properties above. Please ensure Alluxio additional site properties are added
on both HBase Service and Client `hbase-site.xml`.

One key point to note is that when HBase runs on Alluxio it will also write it's WAL files to Alluxio.
These files need to be persisted in the event of a crash. Therefore it is recommended that users specify
`alluxio.user.file.writetype.default=CACHE_THROUGH` or `THROUGH` in `hbase-site.xml`.

Then save the configuration, and the Cloudera Manager will notify you that you should restart the affected
components. Please restart the affected components.

### Running Sample HBase Application

Before running HBase applications, visit HBase Web UI at `http://<hostname>:60010` to confirm that HBase
is running on Alluxio (check the `HBase Root Directory` attribute).
And visit Alluxio Web UI at `http://<hostname>:19999`, click `Browse` and you can see the files HBase stores on Alluxio, including data and WALs.

Then, you can follow the sample HBase application on [Running-HBase-on-Alluxio](Running-HBase-on-Alluxio.html)

## Running CDH Hive

### Configuring HIVE_AUX_JARS_PATH

To run CDH Hive applications with Alluxio, additional configuration is required for the applications.

In the "Hive" section of the Cloudera Manager, in the "Configuration" tab, search for the parameter
"Gateway Client Environment Advanced Configuration Snippet (Safety Valve) for hive-env.sh". For this
parameter, add the following line:

```bash
HIVE_AUX_JARS_PATH=/path/to/alluxio/client/hadoop/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-hadoop-client.jar:${HIVE_AUX_JARS_PATH}
```

It should look something like this:

![CDHHiveEnv]({{ '/img/screenshot_cdh_hive_env_classpath.png' | relativize_url }})

Then save the configuration, and the Cloudera Manager will notify you that you should restart the affected
components. Please restart the affected components.

### Security

For impersonation, Alluxio will need to be configured to allow the `hive` user to impersonate other users. To do this,
add the below property to `alluxio-site.properties` on Alluxio Masters and Workers and then restart the Alluxio cluster.

```bash
alluxio.master.security.impersonation.hive.users=*
```

Note that if `hive.doAs` is disabled, this property is not required.

### Create External Table Located in Alluxio

With the `HIVE_AUX_JARS_PATH` set, Hive can create external tables from files stored on Alluxio. 
You can follow the sample Hive application on [Running-Hive-on-Alluxio](Running-Hive-on-Alluxio.html) to create an
external table located in Alluxio.

### (Optional) Use Alluxio as default file system

Hive can also use Alluxio through a generic file system interface to replace the Hadoop file system.
In this way, the Hive uses Alluxio as the default file system and its internal metadata and intermediate results
will be stored in Alluxio by default. To set Alluxio as the default file system for CDH Hive,
in the "Hive" section of the Cloudera Manager, in the "Configuration" tab, search for the parameter
"hive-site.xml". The search result will contain "Hive Service Advanced Configuration Snippet (Safety Valve) for hive-site.xml"
and "Hive Client Advanced Configuration Snippet (Safety Valve) for hive-site.xml", please add the following property
to both Hive Service and Hive Client hive-site.xml.

```
Name:  fs.defaultFS
Value: alluxio://master_hostname:port
```

It should look something like this:

![CDHHiveSite]({{ '/img/screenshot_cdh_hive_site.png' | relativize_url }})

When using Alluxio as the defaultFS, Hive's warehouse will point to `alluxio://master:19999/user/hive/warehouse`. This
directory should be created and given permissions `hive:hive`. This also allows users to define internal tables on
Alluxio.

### (Optional) Add additional Alluxio Properties for Hive
If there are any Alluxio site properties you want to specify for Hive, add those to `hive-site.xml`
similar to how `fs.defaultFS` was set above. Please ensure Alluxio additional site properties are added
on both Hive Service and Hive Client `hive-site.xml`. Optionally, you might also want to check whether
it is required to add to Hive Metastore Server and HiveServer2 for `hive-site.xml`.

Then save the configuration, and the Cloudera Manager will notify you that you should restart the affected
components. Please restart the affected components.

### Running Sample Hive Application

You can follow the sample Hive application on [Running-Hive-on-Alluxio](Running-Hive-on-Alluxio.html)

## Running CDH Spark

To run CDH Spark applications with Alluxio, additional configuration is required for the applications.

There are two scenarios for the Spark and Alluxio deployment. If you already have the Alluxio Spark
client jars on all the nodes on the cluster, you only have to specify the correct path to for the classpath.
Otherwise, you can allow Spark to distribute the Alluxio Spark client jar to each Spark node for each
invocation of the application.

### Alluxio Spark Client Jar Already on Each Node

If the Alluxio Spark client jar is already on every node, you have to add that path to the classpath for
the Spark driver and executors. In order to do that, use the `spark.driver.extraClassPath` or `--driver-java-options`
and the `spark.executor.extraClassPath` variables. If this 

For `spark-submit` an example looks like the following. (In the example, replace `MASTER_HOSTNAME` with the
actual Alluxio master hostname.)

```bash
$ spark-submit --master yarn --conf "spark.driver.extraClassPath=/path/to/alluxio/client/spark/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-spark-client.jar" --conf "spark.executor.extraClassPath=/path/to/alluxio/client/spark/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-spark-client.jar" --class org.apache.spark.examples.JavaWordCount /opt/cloudera/parcels/CDH/lib/spark/examples/lib/spark-examples-1.6.0-cdh5.8.2-hadoop2.6.0-cdh5.8.2.jar alluxio://MASTER_HOSTNAME:19998/testing/randomtext/
```

And similarly, for `spark-shell`, the following is an example:

```bash
$ spark-shell --master yarn --driver-class-path "/path/to/alluxio/client/spark/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-spark-client.jar" --conf "spark.executor.extraClassPath=/path/to/alluxio/client/spark/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-spark-client.jar"
```

### Distribute Alluxio Spark Client Jar for Each Application

If the Alluxio Spark client jar is not already on each machine, you can use the `--jars` option
to distribute the jar for each application.

For example, using `spark-submit` would look like:

```bash
$ spark-submit --master yarn --jars /path/to/alluxio/client/spark/alluxio-{{site.ALLUXIO_RELEASED_VERSION}}-spark-client.jar --class org.apache.spark.examples.JavaWordCount /opt/cloudera/parcels/CDH/lib/spark/examples/lib/spark-examples-1.6.0-cdh5.8.2-hadoop2.6.0-cdh5.8.2.jar alluxio://MASTER_HOSTNAME:19998/testing/randomtext/
```
