---
layout: global
title: Running Spark on Alluxio in DC/OS
nickname: Apache Spark on DC/OS
group: Data Applications
priority: 7
---

* Table of contents
{:toc}

# Spark on Alluxio DC/OS v2.2.0-{{site.ALLUXIO_RELEASED_VERSION_NUMBER}}

This guide describes how to run [Apache Spark](http://spark.apache.org/) on Alluxio in a DC/OS environment with framework [v2.2.0-{{site.ALLUXIO_RELEASED_VERSION_NUMBER}}](Alluxio-on-DCOS.html#alluxio-dcos-v220-180).

## Prerequisites
- A DC/OS cluster
- A Docker registry on the DC/OS cluster

## Build Spark Docker Image
The DC/OS CLI subcommand `dcos alluxio-enterprise` can be used to build a custom Spark image with the Alluxio client jar and client side configuration required to connect with an Alluxio cluster deployed on DC/OS.

```bash
dcos alluxio-enterprise plan start build-spark-client -p DOCKER_PUBLISH_URL=<registry-host>:<registry-port> -p DOCKER_SPARK_CLIENT_BASE=mesosphere/spark:1.0.6-2.0.2-hadoop-2.6 -p DOCKER_SPARK_DIST_HOME=/opt/spark/dist
```
Parameters:
- `DOCKER_PUBLISH_URL`: Docker registry URL to push the built custom Spark image.
- `DOCKER_SPARK_CLIENT_BASE`: The Docker image containing the desired Spark version.
- `DOCKER_SPARK_DIST_HOME`: Path to Spark home within the Docker image. Alluxio client jar and configuration will be copied over into this location.

## Run a Spark Job on Alluxio

SSH into a DC/OS node and run the Spark Docker image.
```bash
$ docker run -it --net=host <registry-host>:<registry-port>/alluxio/spark-aee /bin/bash
```

From within the Docker image run the Spark shell.
```bash
$ ./bin/spark-shell --master mesos://master.mesos:5050 --conf "spark.mesos.executor.docker.image=registry.marathon.l4lb.thisdcos.directory:5000/alluxio/spark-aee" --conf "spark.mesos.executor.docker.forcePullImage=false" --conf "spark.scheduler.minRegisteredResourcesRatio=1" --conf "spark.scheduler.maxRegisteredResourcesWaitingTime=5s" --conf "spark.driver.extraClassPath=/opt/spark/dist/jars/alluxio-enterprise-1.6.0-spark-client.jar" --conf "spark.executor.extraClassPath=/opt/spark/dist/jars/alluxio-enterprise-1.6.0-spark-client.jar" --executor-memory 1G
```

Note: For locality, please ensure that Spark executors are registered on every node running an Alluxio worker before running a job. You can check the status by visiting the Mesos Web UI at `<DC/OS DnsAddress>/mesos`. Once the cluster is ready for use, you will see the task status change from `Staging` to  `Running` for each Spark executor.

Change `path/to/file` to the path of the file you want to run the Spark count job on.
```bash
scala> sc.setLogLevel("INFO")
scala> val file = sc.textFile("alluxio://master-0-node.alluxio-enterprise.mesos:19998/path/to/file")
scala> file.count()
```

Run the job again to see performance benefits with Alluxio.
Note that a Spark locality level of `NODE_LOCAL` indicates that locality was achieved.
