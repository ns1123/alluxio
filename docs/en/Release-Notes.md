---
layout: global
title: Release Notes
nickname: Release Notes
group: Home
priority: 4
---

# 2.0.0-1.0

We are excited to announce the release of Alluxio Enterprise Edition 2.0.0-1.0. This major release
introduces new data management features, greatly improves usability and resolves several issues.

Versioning scheme: From Alluxio Enterprise Edition v2.0.0-1.0, the versioning scheme has been updated.
The new versioning scheme appends a version suffix to the community version the release is based upon.
For example, enterprise-v2.0.0-1.0 is based on the community version v2.0.0, with a version suffix of 1.0.

Compatibility: Alluxio 2 is not backward compatible with Alluxio 1; please see the
[documentation]({{ 'en/operation/2.x-Upgrade.html' | relativize_url }}) on how to upgrade from
Alluxio 1 to Alluxio 2.

These are some of the highlights of the release.

## Data Driven Applications

### Native Support for AI & ML Workloads with Alluxio POSIX API
A POSIX compatible API has always been a highly requested feature, and the popularity of the API is
second only to the Hadoop API. In Alluxio 2.0.0, we enable a POSIX compatible API through Alluxio FUSE.
Alluxio can be mounted as a local file system volume and accessed through traditional POSIX compatible
clients. This is especially useful for applications not originally built for big data but now require
access to data which has been stored in a data lake, for example, machine learning using Tensorflow.
See the [docs]({{ 'en/compute/Tensorflow.html' | relativize_url }}) for details.

### Enable Better Performance for Object Store with Fast Commits
Compute frameworks which use rename to commit results run much slower on object storages due to rename
being an expensive operation. Alluxio 2.0.0 enables users to write temporary data into Alluxio and
then asynchronously persist the data to the underlying storage. See the
[docs]({{ 'en/advanced/Performance-Tuning.html#optimized-commits-for-compute-frameworks' | relativize_url }})
for more details.

## Core

### Support of Billions of Files with RocksDB Off Heap Metadata Management
Alluxio 2.0.0 provides the option to use RocksDB for storing file system metadata, enabling the namespace
to scale to over one billion files. When running in off heap mode, an on-heap cache is used to serve
more commonly accessed portions of the tree (for example, the root and top level directories), enabling
most of the tree to be accessed at a similar speed as the in-memory metadata storage. Users can still
configure `alluxio.master.metastore=ROCKS` to use the new RocksDB based metadata store.

### Unified Data & Control Plane with gRPC
In Alluxio 2.0.0, we unify RPC framework for both control path and data path by replacing the Thrift/Netty
based RPC framework with gRPC. Previously there were a lot of high level functions built on top of Netty
to support high-performance data streaming, which is now much simplified with the built-in streaming
APIs from gRPC. Code paths in Authentication, flow control and many other important features to support
the legacy hybrid RPC framework are also unified on top of gRPC, which is much easier to maintain and
extend from. All Alluxio RPC services are now defined using Protobuf IDL.

### Ease of HA Deployment with RAFT based Embedded Journal
Alluxio 2.0.0 introduces a new mode for journaling metadata. The embedded journal is a fully contained,
distributed state machine which uses the RAFT consensus algorithm. This allows users to have a fault
tolerant and performant medium for journal storage, independent of 3rd party storage systems and
zookeeper. Users running only on object stores will be able to deploy Alluxio in high availability
mode without incurring a large metadata performance penalty or relying on another distributed storage.
See the [docs]({{ 'en/operation/Journal.html#embedded-journal-configuration' | relativize_url }})
for more details.

## Data Services
Alluxio 2.0.0 includes the Alluxio distributed Data Service, a set of services separate from the
Alluxio masters and workers. The Alluxio Data Service acts as a lightweight distributed compute
framework for internal Alluxio operations.

In this release, the following services are available:

### Policy Driven Ufs Migration
Alluxio Enterprise Edition introduces UFS migration capabilities, defined by policies. Alluxio now
supports mounting multiple UFS URI’s (sub-UFSes) to a single Alluxio mount point. Therefore, for all
the Alluxio paths in this mount point, any single Alluxio URI is backed by more than one UFS path.
Mounting multiple storage systems in Alluxio is enabled by the
[UnionUFS]({{ 'en/advanced/Policy-Driven-Data-Management.md#mounting-multiple-storage-systems-with-union-ufs' | relativize_url }}).

Once the UnionUFS is used for mounting, policies can be defined for UFS migration between the different
sub-UFSes. A UFS migration policy defines an <older than time period> and a list of various operations,
such as REMOVE and STORE. This policy means if a path is older than the <older than time period>, the
following operations will be performed. For example, `ufsMigrate(2d, UFS[HDFS]:REMOVE, UFS[S3]:STORE)`
is a policy definition which means “for all paths which are older than 2 days, the file will be stored
in the S3 sub-UFS alias, and removed from the HDFS sub-UFS alias”. Alluxio’s internal policy engine
continually enforces the policies on the paths. More details and examples can be found in the
documentation for [UFS migration policies]({{ 'en/advanced/Policy-Driven-Data-Management.md#moving-data-with-policies' | relativize_url }}).

### Active Replication
Users can set a min and max replication factor at the file level. The Data Service is used to enforce
this by adding and removing copies in Alluxio storage. See the [docs]( 'en/advanced/Alluxio-Storage-Management.html' | relativize_url }})
for more details.

### Persist and Async Persist
The logic for writing a file synchronously or asynchronously to the under store is handled through
the Data Service enabling better load balancing and error handling. See the
[docs]({{ 'en/advanced/Alluxio-Storage-Management.html#persisting-data-in-alluxio' | relativize_url }}) for more details.

### Cross Under Stores Data Move
Moving files and directories across mount points is now possible by using the Data Service to do the
data transfer. See the [docs]({{ 'en/basic/Command-Line-Interface.html#mv' | relativize_url }}) for more details.

### Distributed Load
The Data Service allows data to be evenly loaded across nodes. See the
[docs]({{ 'en/basic/Command-Line-Interface.html#load' | relativize_url }}) for more details.

## Under Store

### Active HDFS Under File System Sync
Alluxio 2.0.0 integrates with HDFS iNotify to update stale data and metadata in a subscription-based
model instead of the on-demand polling which was previously done. This greatly reduces the period of
time a stale read is possible and improves metadata performance by reducing the number of unnecessary
calls to the backing store. See the
[docs]({{ 'en/advanced/Namespace-Management.html#metadata-active-sync-for-hdfs' | relativize_url }})
for more details.

### Web (HTTP & HTTPS) Under Store Support
Users can now bring in data even from web-based data sources to aggregate in Alluxio to perform
their analytics. Any web location with files can be simplified pointed to Alluxio to be pulled in as
needed based on the query or model run. See the
[docs]({{ 'en/ufs/WEB.html' | relativize_url }}) for more details.

## DevOps

### AWS Elastic Map Reduce (EMR) service integration
As users move to cloud services to deploy analytical and AI workloads, services like AWS EMR are
increasingly used. Alluxio can now be seamlessly bootstrapped into an AWS EMR cluster making it
available as a data layer within EMR for Spark, Presto and Hive frameworks. Users now have a
high-performance alternative to cache data from S3 or remote data while also reducing data copies
maintained in EMR. See the [docs]({{ 'en/compute/AWS-EMR.html' | relativize_url }}) for more details.

### Better Configuration Controls with Path Level Configuration
Users can specify default configuration on a per path basis, allowing administrators to have finer
grained control on the settings clients use to access data. Clients may still override the path level
configuration with client-side configuration. Path level configuration greatly simplifies the
configuration necessary on client applications, for example, instead of having application logic
switch write types, path level configuration can be used to ensure all writes to temporary directories
are only written to memory. See the [docs]({{ 'en/basic/Configuration-Settings.html#path-defaults' | relativize_url }})
for more details.

### Improved Kubernetes Deployment Support
Access Alluxio using the POSIX API from any application container in Kubernetes by simply mounting
a hostPath volume after running the alluxio/alluxio-fuse container as a DaemonSet. See the
[docs]({{ 'en/deploy/Running-Alluxio-On-Kubernetes.html' | relativize_url }}) for more details.

### 3rd Generation WebUI
The Alluxio web UI has been updated to use ReactJS and REST APIs. The overall user experience has
been improved. The updated UI uses a dark theme with a better color palette. The set of cluster
metrics and aggregated system metrics provided through the master has been improved. We have also
added support for rendering timeseries on the UI. See the 
[docs]({{ 'en/basic/Web-Interface.html' | relativize_url }}) for more details.
