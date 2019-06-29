---
layout: global
title: Under Store Replication
nickname: Under Store Replication
group: Advanced
---

* Table of Contents
{:toc}

Data replication is the process of copying data from one location to another so you have multiple up-to-date copies of
data.
For disaster recovery (DR), replication usually occurs between multiple storage systems in different physical
locations, so that in case one of the storage locations becomes unavailable, data can be served from the other locations.
The Under Store Replication feature allows Alluxio to be used as a mechanism for performing data replication as well as
serving data from multiple locations, thus enabling DR.

This page describes how Alluxio can be used to transparently replicate data to multiple UFS locations when
writing data, as well as to serve the data when reading even during temporary unavailability of some of the UFSes.

## How to Use Under Store Replication

The under store can be mounted to Alluxio using the `alluxio-fork://<id>` URI as an argument for the Alluxio
`./bin/alluxio mount` command.
The `<id>` represents a user-specified name for the UFS and helps Alluxio distinguish between different under stores.

The following mount options can be used in conjunction with the `alluxio-fork://<id>` argument to then specify the
underlying UFSes and their options:

- `alluxio-fork.<ufs-id>.ufs=<url>` can be used to specify the under store URL `<url>`, where `<ufs-id>`
represents the user-specified name for the underlying under store
- `alluxio-fork.<ufs-id>.option.<key>=<value>` (optional) can be used to set any `<key>=<value>` options for the under
store with a matching identifier. Providing this property is optional.

## Example

```bash
# pre-requisite: two HDFS clusters running on <host1> and <host2>

# 1. create HDFS endpoints to mount into Alluxio
/path/to/hadoop dfs -mkdir hdfs://<host1>:9000/alluxio
/path/to/hadoop dfs -mkdir hdfs://<host2>:9000/alluxio

# 2. mount HDFS endpoints to Alluxio
./bin/alluxio fs mkdir /mnt
./bin/alluxio fs mount /mnt/fork alluxio-fork://test/ -option alluxio-fork.A.ufs=hdfs://<host1>:9000/alluxio -option alluxio-fork.B.ufs=hdfs://<host2>:9000/alluxio

# 3. write data to Alluxio and persist it to the HDFS endpoints
dd if=/dev/urandom of=test-file count=200 bs=1048576
./bin/alluxio fs copyFromLocal ./test-file /mnt/fork/test-file
./bin/alluxio fs persist /mnt/fork/test-file
/path/to/hadoop dfs -ls hdfs://<host1>:9000/alluxio/test-file
/path/to/hadoop dfs -ls hdfs://<host2>:9000/alluxio/test-file

# 4. free the file from Alluxio storage, stop one of the HDFS clusters, and check that Alluxio can still read the file from the remaining HDFS cluster
./bin/alluxio fs free /mnt/fork/test-file
ssh <host1> /path/to/stop-dfs.sh
./bin/alluxio fs copyToLocal /mnt/fork/test-file test-file2
```

## Semantics

A write operation (both data and metadata) over the `alluxio-fork://` under store is synchronously performed against all
of the underlying under stores in parallel and only succeeds if all of the underlying operations succeed.
If a write operation fails, the state of the file in the under stores is undefined.
The state of the file in each UFS may vary depending on the type of failure.
Most of the time write failures should be recoverable, but the UFS may have corrupt or incomplete data.

A read operation (both data and metadata) over the `alluxio-fork://` under store is synchronously performed against the
underlying under stores, one by one (in alphabetical order of the user-specified under store names), until an underlying
operation succeeds.
The operation fails only if all of the underlying operations fail.

The behavior of Alluxio operations over the `alluxio-fork://` UFS is defined only when all of the underlying
under stores are in sync; that is both the data and the metadata of the underlying under stores is the same.
If the underlying under stores are not in sync, then the behavior of Alluxio is undefined.
