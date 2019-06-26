---
layout: global
title: Native Kerberos Security Setup
nickname: Native Kerberos (MIT)
group: Advanced
---

* Table of Contents
{:toc}

This documentation describes how to set up an Alluxio cluster with
MIT native [Kerberos](http://web.mit.edu/kerberos/), running on an AWS EC2 Linux cluster as an example.
This doc uses the example of hostname-associated principal setup. If you would like to use unified
principal across all the Alluxio service nodes, please set `alluxio.security.kerberos.unified.instance.name`
and make sure the principal instance name matches this configuration property.

The default Java GSS implementation relies on JAAS KerberosLoginModule for initial credential acquisition.
In contrast, when native platform Kerberos integration is enabled, the initial credential
acquisition should happen prior to calling JGSS APIs, e.g. through `kinit`.
When enabled, Java GSS would look for native GSS library using the operating system specific name,
e.g. Solaris: `libgss.so` vs Linux: `libgssapi.so`. If the desired GSS library has a different name or is not
located under a directory for system libraries, then its full path should be specified using the system
property `sun.security.jgss.lib`.

## Prerequisites
 * There is an existing MIT KDC (Key Distribution Center).
 * Krb5 client library is installed on the machines running Alluxio servers and clients.
 * `ALLUXIO.COM` is the example realm name in this doc.
 * `alluxio` is the example Alluxio service name in this doc.
 * Each Alluxio service node (Masters and Workers) uses service principal named `alluxio/<HOSTNAME>@ALLUXIO.COM`.
 * In all Alluxio service nodes, Kerberos credentials for `alluxio/<HOSTNAME>@ALLUXIO.COM` already exist (e.g. through `kinit`).
 * The service name `alluxio` is a valid user in Linux operating system.
 * `<HOSTNAME>` can be set to user-qualified hostname, such as `user.full.machine.host.name`

## Alluxio Configuration
When installing Alluxio, you can enable
Kerberos security for Alluxio by setting up configuration properties in
`alluxio-site.properties`.

```properties
alluxio.security.authentication.type=KERBEROS
alluxio.security.kerberos.service.name=alluxio
alluxio.master.hostname=<MASTER_HOSTNAME>
```

You also need to set the worker’s hostname on the worker nodes. Please make sure the
`WORKER_HOSTNAME` set in Alluxio site properties matches with the `<HOSTNAME>` part of the service
principal. Otherwise, Kerberos authentication will fail. You can set the worker hostname in
`alluxio-site.properties`.

```properties
alluxio.worker.hostname=<WORKER_HOSTNAME>
```

Note:

- `alluxio.security.kerberos.service.name` is required to specify the Alluxio Service Principal
service name. It is assumed that there is a present Kerberos ticket with the principal `<primary>/<instance>@REALM.COM`
whose `<primary>` part matches with `alluxio.security.kerberos.service.name`.

- There is an optional property called `alluxio.security.kerberos.unified.instance.name`. If specified,
all the Alluxio servers will share the same principal. For example, if the unified instance name is set to
`alluxio.security.kerberos.unified.instance.name=cluster`, then the master and worker principals will be the same,
and will be `alluxio/cluster@ALLUXIO.COM`.

JGSS native Kerberos integration requires an environment variable (i.e. `KRB5_KTNAME`) to
find the keytab file for Alluxio processes. If this environment variable is not set globally, you can add the following line
to `${ALLUXIO_HOME}/conf/alluxio-env.sh` so that `KRB5_KTNAME` refers to the keytab file for `alluxio/localhost@ALLUXIO.COM`.

```bash
# Replace </path/to/keytab> with the path to the keytab of Alluxio service principal, e.g. alluxio/localhost@ALLUXIO.COM
export KRB5_KTNAME=</path/to/keytab>
```

Furthermore, to use JGSS native Kerberos integration, the following Java system properties must be set. If they
are not already set system-wide, you can add them to `ALLUXIO_JAVA_OPTS` in
`{ALLUXIO_HOME}/conf/alluxio-env.sh`:

```properties
ALLUXIO_JAVA_OPTS+=" -Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false "
```

To access a Kerberized Alluxio cluster, Alluxio clients require the same configuration.

Using the JGSS native Kerberos implementation requires populating the kerberos ticket cache before starting
Alluxio processes. Therefore, before you start any Alluxio server process (master or worker), you must
`kinit` with the appropriate service principal, and with the OS user starting the Alluxio process. For example,
if the Alluxio master will be started by OS user `alluxioadmin`, the `alluxioadmin` can start Alluxio by:

```bash
$ kinit -kt </path/to/keytab> alluxio/localhost@ALLUXIO.COM
$ ./bin/alluxio-start.sh master
```

Similarly, the worker can be started by:

```bash
$ kinit -kt </path/to/keytab> alluxio/localhost@ALLUXIO.COM
$ ./bin/alluxio-start.sh worker SudoMount
```

## Kerberos-enabled Alluxio Integration with Secure-HDFS as UFS

Please visit the guide for Secure HDFS UFS.

Kerberos-Security-Setup.html#kerberos-enabled-alluxio-integration-with-secure-hdfs-as-ufs

## Running Spark with Alluxio Kerberized with native integration
Follow the [Running-Spark-on-Alluxio](Running-Spark-on-Alluxio.html) guide to set up
`SPARK_CLASSPATH`. In addition, the following items should be added to make Spark aware of Kerberos
configurations:

- Copy Alluxio site configuration `{ALLUXIO_HOME}/conf/alluxio-site.properties` to
`{SPARK_HOME}/conf` for Spark to pick up Alluxio configurations such as Kerberos related flags.

- When launching Spark shell or jobs, please add
`-Dsun.security.jgss.native=true -Djavax.security.auth.useSubjectCredsOnly=false`
 in `spark.executor.extraJavaOptions` and `spark.driver.extraJavaOptions`

## FAQ

Please visit the [general Kerberos FAQ](Kerberos-Security-Setup.html#faq) for additional questions.

When using the native libraries, you can set an environment variable `KRB5_TRACE=/tmp/path/to/log`.
Additionally, set the Kerberos debug level with:

```
-Dsun.security.krb5.debug=true
```

### Cannot add private credential to subject with JGSS
This is typically because the required pre-existing Kerberos credential is not valid.
Please run `klist` to double check.

### Unable to Obtain Password from User
You will see this only if the JGSS system property is not setup correctly. Alluxio falls back to the
JAAS Kerberos login with ticket cache and keytab file if `sun.security.jgss.native` is not enabled.
