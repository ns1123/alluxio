---
layout: global
title: Alluxio on DC/OS
nickname: DC/OS
group: Deploying Alluxio
priority: 5
---

Alluxio DC/OS framework is an automated service that makes it easy to deploy and manage Alluxio on [Mesosphere DC/OS](http://dcos.io). The Alluxio DC/OS frameworks are versioned with an a.b.c-x.y.z format, where a.b.c is the version of the service management layer and x.y.z indicates the version of Alluxio. For instance, 2.2.0-{{site.ALLUXIO_RELEASED_VERSION_NUMBER}} indicates version 2.2.0 of the service management layer and version {{site.ALLUXIO_RELEASED_VERSION_NUMBER}} of Alluxio.

* Table of contents
{:toc}

# Alluxio DC/OS v2.2.0-{{site.ALLUXIO_RELEASED_VERSION_NUMBER}}

**Compatible Alluxio Version(s):** v{{site.ALLUXIO_RELEASED_VERSION_NUMBER}}

**Compatible DC/OS Version(s):** v1.10.0

## Prerequisites

- A DC/OS cluster
- A Docker registry on the DC/OS cluster (optional)

For instructions on how to setup a DC/OS cluster, visit the Mesosphere [website](https://dcos.io/install/). Note that a Docker registry is only required to host custom images, such as in the case for hosting a custom Spark image embedded with the Alluxio client library.

Before installing Alluxio on a DC/OS cluster, verify that the DC/OS hosts satisfy the following assumptions:
- `Bash` shell is available
- `/dev/shm` is a `tmpfs` location and the Alluxio service has read and write access. This location is used by the Alluxio workers as the primary storge tier.
- Alluxio service is able to create `/tmp/domain/<service-name>`; where `<service-name>` is the service name for any given Alluxio instance on DC/OS. This location is used to enable domain socket based short circuit I/O to write data to local Alluxio storage through a worker process instead of the client.
- If using `KERBEROS` authentication mode or connecting to secure HDFS, `krb5` libraries are pre-installed on the hosts.
- If building Alluxio client Docker images, hosts have access to a Docker registry containing base images.

## Install and Customize
Alluxio can be installed either using the DC/OS CLI or Web UI. This section provides instructions for both.

After obtaining an Alluxio Enterprise Edition license, base64 encode the license. The encoded license will be required in following steps.

```bash
# Assuming license file is named alluxio-enterprise-license.json
$ cat alluxio-enterprise-license.json | base64 | tr -d "\n"
```

### Install using CLI
To install Alluxio using DC/OS CLI, first create a configuration file as follows:

```bash
tee alluxio-config.json  <<-'EOF'
{
  "service": {
    "name": "alluxio-enterprise",
    "alluxio-distribution": "all",
    "license": "<license>",
    "log-level": "INFO",
    "ufs-address": "<under-storage-address>",
    "user": "root",
    "worker-count": 1
  }
}
EOF
```

Note: Additional configuration may be required for the choice of under storage (look [here](Alluxio-on-DCOS.html#under-storage-configuration)). In case not using the default, update the configuration `service.alluxio-distribution`.

Once the configuration is created, Alluxio can be installed as follows:
```bash
$ dcos package install --yes --options=alluxio-config.json alluxio-enterprise
```

### Install using Web UI
1. Find Alluxio in the DC/OS Universe and click `Advanced Installation`.
1. Enter the base64 encoded license obtained previously.
1. Update the `ufs-address` field.
1. Review configuration and install.

Note: Additional configuration may be required for the choice of under storage (look [here](Alluxio-on-DCOS.html#under-storage-configuration)).

### Check Deployment Status
You can check the Alluxio deployment status in the DC/OS Web UI by visiting `Services > alluxio-enterprise > Instances`. Once the cluster is ready for use, you will see a tasks named `master-<i>-node`, `master-<i>-job-node`, `worker-<j>-node` aand `worker-<j>-job-node` with status `Running`; where `<i>` is the master instance index and `<j>` is the worker instance index.
## Under Storage Configuration

### HDFS

To configure HDFS as under storage for Alluxio, configure the following property if a single name node is used:
```properties
  "service": {
    "ufs-address": "hdfs://<name-node>:<port>/<path>"
  },
  "hdfs": {
    "root-version": "<hdfs-version>"
  }
```

For HDFS HA, replace the single name node configuration with name service address and configure an additional `endpoint` configuration. The endpoint URL is the prefix for `core-site.xml` and `hdfs-site.xml`.
```properties
  "service": {
    "ufs-address": "hdfs://<name-service>/<path>"
  },
  "hdfs": {
    "endpoint": "http://<endpoint-hostname>/<path>"
  }
```

To put the Alluxio journal on HDFS, add the following properties:
```properties
 "master": {
    "journal-folder": "hdfs://<name-node-or-service>/<path>"
  },
  ...
  "hdfs": {
    "journal-version": "<hdfs-version>"
  },
```

### S3A
Access credentials must be provided to configure an Amazon S3 bucket as under storage. Add the following configuration:
```properties
  "service": {
    "ufs-address": "s3a://<bucket>/<folder>"
  },
  "s3a": {
    "aws-access-key": "<access-key>",
    "aws-secret-key": "<secret-key>"
  }
```

## Additional Features

### High Availability Mode
To make Alluxio run in HA / fault tolerant mode, deploy at least two Alluxio master nodes, configure zookeeper address and a shared journal location (e.g., HDFS or NFS):
```properties
  "service": {
    "master-count": 3,
  },
  "master": {
    "journal-folder": "hdfs://hdfs/journal"
  },
  "zookeeper": {
    "enabled": true,
    "address": "zk-1.zk:2181,zk-2.zk:2181,zk-3.zk:2181"
  }
```

### Kerberos Security

- Obtain a keytab. The following example uses a keytab with principal `hdfs/alluxio@ALLUXIO.COM`. In this example, it is assumed that all the Alluxio servers use a shared Kerberos service principal, with a unified instance name `alluxio`.

- Create an Enterprise DC/OS secret allowing access to `<service-name>`. The default service name is `alluxio-enterprise`.

```bash
base64 -i hdfs.keytab > hdfs.keytab.base64-encoded
dcos security secrets create <service-name>/__dcos_base64__alluxio-keytab -f hdfs.keytab.base64-encoded

dcos security secrets create <service-name>/krb5-conf -f krb5.conf
```
Note: The keytab is `base64` encoded before adding it to the secret store.

- Add the following properties to configure the Alluxio service:

```properties
  "security": {
    "authentication-type": "KERBEROS",
    "authorization-permission-enabled": true,
    "keytab-secret": "<service-name>/__dcos_base64__alluxio-keytab",
    "kerberos-primary": "hdfs",
    "kerberos-instance": "alluxio",
    "kerberos-realm": "ALLUXIO.COM",
    "krb5-secret": "alluxio-enterprise/krb5-conf"
  }
```
Also make sure the HDFS endpoint is configured to fetch `hdfs-site.xml` and `core-site.xml`.

### REST API
First, make sure the command line tools for Alluxio is installed.
```bash
dcos package install alluxio-enterprise --cli
```

To use Alluxio REST API, deploy the Alluxio Proxy server:
```bash
dcos alluxio-enterprise plan start proxy
```

Once deployed, try out a few operations using the REST API:
- Create a directory `/hello/world`
```bash
curl -v -H "Content-Type: application/json" -X POST -d '{"recursive":"true"}' http://proxy-0-node.alluxio-enterprise.mesos:19997/api/v1/paths//hello/world/create-directory
```
- List a directory
```bash
curl -v -X POST http://proxy-0-node.alluxio-enterprise.mesos:19997/api/v1/paths//hello/list-status
```

### Tiered Storage
The default configuration will setup Alluxio with memory storage only. You can customize it to include more than one tier of storage:
```properties
  "worker": {
    "mem-tier-enabled": true,
    "mem-ramdisk": 2048,
    "disk-tier-1-enabled": true,
    "disk-tier-1-type": "ROOT",
    "disk-tier-1-size": 8192
  }
```
The above example will request 8GB of disk storage per worker in addition to 2GB of memory storage.

Note that currently configuring Alluxio to request a specific type (HDD/SSD) of storage is not supported. However, DC/OS disk type can be specified to allocate disk space on either ROOT or MOUNT volumes.

### Advanced Configuration
- Arbitrary configuration can be specified using `advanced.extra-java-opts`. These will override any existing properties in `alluxio-site.properties` rendered using the config json.

- Custom resources can be downloaded into the sandbox using `advanced.custom-resources`. This feature can be used to download configurations for Multi-HDFS as well as multiple keytabs in case separate server and under storage principals are used.
```properties
  "advanced": {
    "extra-java-opts": "-Dalluxio.property1=common1 -Dalluxio.property2=common2",
    "custom-resources": "custom_file1;http://<hostname>/<file1> custom_file2;http://<hostname>/<file2>"
  }
```

## Validate Deployment

To test the cluster, build the Alluxio client Docker image. Omit the publish URL to not publish to a registry.
```bash
$ dcos package install alluxio-enterprise --cli

$ dcos alluxio-enterprise plan start build-cli -p DOCKER_PUBLISH_URL=<registry-hostname>:<registry-port> -p DOCKER_ALLUXIO_CLIENT_BASE=openjdk:8-jre
```

If the Docker image was published to a registry, access the image on any DC/OS node.
```bash
dcos node ssh --master-proxy --leader
docker run -it --net=host <registry-hostname>:<registry-port>/alluxio/aee /bin/bash
```

If not published, the docker image `aee` can be accessed on the node that built the image. To identify the node IP, look at the service UI which ran a task named `docker-alluxio`.
```bash
$ dcos node # Note mesos id for above IP
$ dcos node ssh --master-proxy --mesos-id <mesos-id>
$ docker run -it --net=host aee /bin/bash
```

From within the docker container `aee`, the Alluxio CLI can be accessed to validate the deployment.
```bash
$ ./bin/alluxio runTests
$ ./bin/alluxio fs ls /
```

Now that Alluxio is running on your DC/OS cluster, you could choose to run [Spark on Alluxio](Spark-on-DCOS.html).
