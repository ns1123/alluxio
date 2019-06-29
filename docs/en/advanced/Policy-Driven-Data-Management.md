---
layout: global
title: Policy-Driven Data Management
nickname: Policy-Driven Data Management
group: Advanced
---

* Table of Contents
{:toc}

Moving data from one storage system to another, while keeping computation jobs running correctly is a common challenge
for many Alluxio users. Traditionally this is done by manually copying the data to the destination, asking every user of
the data to update their applications and queries to use the new URI, waiting until all the updates are complete, then
finally removing the original copy. With Alluxio, this can be done seamlessly by mounting multiple storage systems to a
single mount point using a union UFS.

For example, if a user wants to move data from `hdfs://hdfs_cluster/data/` to `s3://user_bucket/`, they can mount both
locations using a union-UFS to Alluxio under path `/union/`, and create a migration policy to move data from HDFS to S3.
Alluxio will enforce the policy and automatically move the data, while users can keep using the same Alluxio path
`alluxio://host/union` to access their data, without worrying that their computation job will fail after the data is moved.

On a union UFS, each under storage system is mounted as a sub-UFS. A path in Alluxio is mapped to one or more
corresponding files in the mounted sub-UFSes. When a file is written to Alluxio, it can be configured to be written to
one or more sub-UFSes. When a file is read from Alluxio, it can be read transparently from any sub-UFS mounted with union
UFS. This allows user to move data among storage systems easily while accessing data with a single Alluxio URI
regardless which storage system the file is in.

This page describes the instructions to configure a union UFS to mount multiple storage systems, and setup policies
to move data among them.

## Mounting Multiple Storage Systems with Union UFS

Similar to mounting any other under storage system, a union UFS can be mounted using `alluxio fs mount` command:

```bash
./bin/alluxio fs mount \
   --option alluxio-union.<UFS_A_ALIAS>.uri=<UFS_A_URI> \
   --option alluxio-union.<UFS_A_ALIAS>.option.<KEY>=<VALUE> \
   --option alluxio-union.<UFS_B_ALIAS>.uri=<UFS_B_URI> \ 
   --option alluxio-union.<UFS_B_ALIAS>.option.<KEY>=<VALUE> \
   --option alluxio-union.priority.read=<ALIAS_1>,<ALIAS_2>,<ALIAS_3>... \
   --option alluxio-union.collection.create=<UFS_ALIASES>  \
   <ALLUXIO_MOUNT_PATH> union://<UNION_UFS_AUTHORITY>/
```

In the command above:

- `alluxio-union.<UFS_ALIAS>.uri` specifies a URI of a sub under storage system(sub-UFS) to be mounted. `<UFS_ALIAS>`
 should be replaced with an alias representing the sub-UFS. Users can specify multiple UFSs to be mounted with
 different aliases. For example, `alluxio-union.hdfs.uri=hdfs://local_hdfs/user_a/` tells the union UFS to mount a
 sub-UFS at `hdfs://local_hdfs/user_a/` with the alias `hdfs`. The alias must be in lower case.
- `alluxio-union.<UFS_ALIAS>.option.<KEY>` specifies an optional UFS option for a specific sub-UFS. `<UFS_ALIAS>`
 denotes an alias of sub-UFS defined by `alluxio-union.<UFS_ALIAS>.uri`, `<KEY>` denotes the option key to be set
 to the target sub-UFS. For example, `alluxio-union.s3.option.aws.accessKeyId=MYKEYID` sets `aws.accessKeyId` option
 for sub-UFS `s3` to value `MYKEYID`.
- `alluxio-union.priority.read` specifies an ordered list of UFS aliases to set the read priority of sub-UFSes.
 When a file is read from a union UFS, it will be attempted on sub-UFSes in the same order as the aliases in this list.
 The first sub-UFS with the file available for reading will be used. All sub-UFS aliases must appear in this list.
- `alluxio-union.collection.create` specifies an unordered list of UFSes to write new files to.
 When a new file is written to a union UFS, it will be written to all the UFSes whose aliases specified in this list.
- `<ALLUXIO_MOUNT_PATH>` specifies the Alluxio path where the union UFS is mounted.
- `<UNION_UFS_AUTHORITY>` specifies a unique authority of the mounted union UFS. It can be empty or any arbitrary name.
 This is used to identify a specific union UFS.

For example, the following command can be used to mount an HDFS directory and an S3 bucket under the Alluxio path `/union`:

```bash
./bin/alluxio fs mount \
   --option alluxio-union.hdfs.uri=hdfs://local_hdfs/user_a/ \
   --option alluxio-union.hdfs.option.alluxio.underfs.hdfs.configuration=/opt/hdfs/core-site.xml:/opt/hdfs/hdfs-site.xml \
   --option alluxio-union.s3.uri=s3://mybucket/ \ 
   --option alluxio-union.s3.option.aws.accessKeyId=MYKEYID \
   --option alluxio-union.s3.option.aws.secretKey=MYSECRETKEY \
   --option alluxio-union.priority.read=hdfs,s3 \
   --option alluxio-union.collection.create=s3  \
   /union union://union_ufs_1/
```

## Reading and Writing Data on Union UFS

Data on a union UFS can be accessed just like any other UFS. Files that exist in any of the sub-UFSes will show up in the
mount point. For example, if sub-UFS `a` contains a file at path `/a/b` and another one at `/c`, and sub-UFS `b` contains
a file at path `/a/d` and and a file at `/c`, then the union UFS will expose a directory structure as follows:

```
+-a
| +b
| +d
|
+-c
```

When file data is read from a union UFS, it will be read from the sub-UFS of the highest priority which contains the file.
In the above example, if sub-UFS `a` has the highest priority, then reading data from `/c` will end up
reading from sub-UFS `a`. Reading data from `/a/d` will end up reading from sub-UFS `b` since only `b` has the file.

When file data is written to a union UFS, it will be written to all sub-UFSes specified in the
`alluxio-union.collection.create` property. A write request will complete after the file is completed on all target
sub-UFSes. If one of the sub-UFSes fails to write the file, the write request will fail.

## Moving Data with Policies

Files in a union UFS can be moved between sub-UFSes by setting policies. A policy can be added by the following command:

```bash
./bin/alluxio fs policy add <PATH> "ufsMigrate(<OLDER_THAN_TIME_PERIOD>, <LOCATION>:<OPERATION>, ...)"
```

In the command above:

- `<PATH>` specifies an Alluxio path where the policy will apply.
- `<OLDER_THAN_TIME_PERIOD>` specifies a time period that the policy will be executed if the target file is older than it.
  Use `s`, `m`, `h`, `d` to indicate time unit seconds, minutes, hours, and days.
  For example, `ufsMigrate(30m, UFS[a]:STORE)` sets a policy to execute after a file is older than 30 minutes.
- `<LOCATION>:<OPERATION>` specifies a file operation to be executed.
  - `<LOCATION>` specifies the target storage of the operation. Use `UFS[<UFS_ALIAS>]` to refer to a sub-UFS.
  - `<OPERATION>` specifies how the data is affected on this storage location. It can be either `STORE` to indicate the
   file data should be stored on this location, or `REMOVE` to indicate that data should be removed from this location.
   
For example, if we have a union UFS mount point at Alluxio mount point `/union/` with the following options:

```properties
alluxio-union.hdfs.uri=hdfs://local_hdfs/user_a/
alluxio-union.s3.uri=s3://mybucket/
```

the command below will add a policy to move files under the Alluxio path `/union/mydir` from the corresponding 
HDFS path `hdfs://local_hdfs/user_a/mydir` to the S3 path `s3://mybucket/mydir` once they are older than 2 days:

```bash
./bin/alluxio fs policy add /union/mydir "ufsMigrate(2d, UFS[hdfs]:REMOVE, UFS[s3]:STORE)"
```

After a policy is added, it will be executed in the background after all its requirements are met. Users can always access
files with their Alluxio URIs regardless whether they are migrated with a policy or not. A policy can also be added to
a mount point using the `--policy <POLICY_DESCRIPTION>` option in `alluxio fs mount` command, for example:

```bash
./bin/alluxio fs mount \
  --option alluxio-union.a.uri=/tmp/ufs1/ \
  --option alluxio-union.b.uri=/tmp/ufs2/ \
  --option alluxio-union.priority.read=a,b \
  --option alluxio-union.collection.create=a \
  --policy "ufsMigrate(2d, UFS[a]:REMOVE, UFS[b]:STORE)"
  /union/ union://test/
```
will mount a union UFS and add a policy to migrate files inside the mount point at `/union/`.

 
To list all policies, use the following command:

```bash
./bin/alluxio fs policy list"
```

To remove a policy so it is no longer executed, use the following command:

```bash
./bin/alluxio fs policy remove <POLICY_NAME>"
```
where `<POLICY_NAME>` should be the name of the policy that is listed in the `policy list` command.

## Example: Running Alluxio Locally with Union UFS

First, start the Alluxio servers locally:

```bash
./bin/alluxio format
./bin/alluxio-start.sh local
```

If your ramdisk is not mounted, likely because this is the first time you are running Alluxio, you may need to start
Alluxio with the `SudoMount` option.

```bash
$ bin/alluxio-start.sh local SudoMount
```

This will start one Alluxio master and one Alluxio worker locally. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Run a simple example program:

```bash
./bin/alluxio runTests
```

If the test fails with permission errors, make sure that the current user (`${USER}`) has
read/write access to the local directory mounted to Alluxio. By default,
the login user is the current user of the host OS. To change the user, set the value of
`alluxio.security.login.username` in `conf/alluxio-site.properties` to the desired username.

After this succeeds, create two local directories to be used as sub-UFSes. In this example we will use `/tmp/ufs1`
and `/tmp/ufs2`:

```bash
mkdir /tmp/ufs1
mkdir /tmp/ufs2
```
 
Mount the union UFS with the local sub-UFSes using the following command:

```bash
./bin/alluxio fs mount \
  --option alluxio-union.a.uri=/tmp/ufs1/ \
  --option alluxio-union.b.uri=/tmp/ufs2/ \
  --option alluxio-union.priority.read=a,b \
  --option alluxio-union.collection.create=a \
  /union/ union://test/
```

Setup a policy to move from UFS `a` to UFS `b``:

```bash
./bin/alluxio fs policy add /union/ "ufsMigrate(1m, UFS[a]:REMOVE, UFS[b]:STORE)" 
```

To test the mounted union UFS, run the following command to copy a new file to the mount point:
```bash
./bin/alluxio fs copyFromLocal LICENSE /union
```
After the command finishes, you should find the `LICENSE` file in local directory `/tmp/ufs1`, but not in `/tmp/ufs2`.

Wait a minute for the policy to be executed, then check the local directories again. The file should now be moved from
`/tmp/ufs1` to `/tmp/ufs2`.

Stop Alluxio by running:

```bash
./bin/alluxio-stop.sh local
```