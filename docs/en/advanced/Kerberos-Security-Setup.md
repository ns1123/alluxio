---
layout: global
title: Kerberos Security Setup
nickname: Kerberos (Java)
group: Advanced
---

* Table of Contents
{:toc}

This documentation describes how to set up an Alluxio cluster with
[Kerberos](http://web.mit.edu/kerberos/) security, running on an AWS EC2 Linux machine *locally* as an example.
To set up a cluster on multiple nodes, please replace the host field (`localhost`) in Kerberos
principals to `<your.cluster.name>`. Or use the hostname-associated principal name and unset the
`alluxio.security.kerberos.unified.instance.name`.

Some frequently seen problems and questions are listed at the end of the document.

## Setup Key Distribution Center (KDC)

### Linux

When setting up Kerberos, install the [KDC](http://www.zeroshell.org/kerberos/Kerberos-definitions/#1.3.5) first.
If it is necessary to set up KDC slave servers for the purpose of high-availability (HA), install the KDC master first.

WARNING: It is best to install and run KDCs on secured and dedicated machine with limited access.
If your KDC is also a file server, FTP server, Web server, or even just a client machine,
someone who obtains root access through a security hole in any of those components can potentially
gain access to the Kerberos database.

First, install all Kerberos required packages following this [guide](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/installing-kerberos.html)

Then please follow this [guide](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/Configuring_a_Kerberos_5_Server.html)
to configure a KDC server on Linux.

Here is a sample Alluxio KDC `/etc/krb5.conf`:

```properties
[logging]
 default = FILE:/var/log/krb5libs.log
 kdc = FILE:/var/log/krb5kdc.log
 admin_server = FILE:/var/log/kadmind.log

[libdefaults]
 dns_lookup_realm = false
 ticket_lifetime = 24h
 renew_lifetime = 7d
 forwardable = true
 rdns = false
 default_realm = ALLUXIO.COM

[realms]
ALLUXIO.COM = {
 kdc = <KDC public IP or DNS address>
 admin_server = <KDC public IP or DNS address>
}

[domain_realm]
 .alluxio.com = ALLUXIO.COM
 alluxio.com = ALLUXIO.COM
```

Edit `/var/kerberos/krb5kdc/kdc.conf` by replacing `EXAMPLE.COM` with `ALLUXIO.COM`.

Note: after the KDC service is up, please make sure the firewall settings
(or Security Group on EC2 KDC machine) is correctly set up with the following ports open:
(You can also disable some service ports as needed.)

```
     ftp           21/tcp           # Kerberos ftp and telnet use the
     telnet        23/tcp           # default ports
     kerberos      88/udp    kdc    # Kerberos V5 KDC
     kerberos      88/tcp    kdc    # Kerberos V5 KDC
     klogin        543/tcp          # Kerberos authenticated rlogin
     kshell        544/tcp   cmd    # and remote shell
     kerberos-adm  749/tcp          # Kerberos 5 admin/changepw
     kerberos-adm  749/udp          # Kerberos 5 admin/changepw
     krb5_prop     754/tcp          # Kerberos slave propagation

     eklogin       2105/tcp         # Kerberos auth. & encrypted rlogin
     krb524        4444/tcp         # Kerberos 5 to 4 ticket translator
```

### Windows Server

Set up an [Active Directory](https://msdn.microsoft.com/en-us/library/bb742424.aspx) service with [KDC](https://msdn.microsoft.com/en-us/library/windows/desktop/aa378170(v=vs.85).aspx).

## Add Principals And Generate Keytab Files in KDC

### Linux

On the KDC node (not the Alluxio nodes), start the Kerberos admin console (root permission required):

```bash
$ sudo kadmin.local
```

If this command succeeds, you will see a message similar to the following, followed by an
interactive *kadmin* shell. (The *kadmin.local:* at the beginning of each line is part of
the kadmin shell prompt, and your commands should follow.)

```bash
Authenticating as principal root/admin@ALLUXIO.COM with password.
kadmin.local:
```

First, create principals for Alluxio servers and clients:

```bash
kadmin.local: addprinc -randkey alluxio/localhost@ALLUXIO.COM
kadmin.local: addprinc -randkey client/localhost@ALLUXIO.COM
kadmin.local: addprinc -randkey foo/localhost@ALLUXIO.COM
```

Second, create keytab files for those principals:

```bash
kadmin.local: xst -norandkey -k alluxio.keytab alluxio/localhost@ALLUXIO.COM
kadmin.local: xst -norandkey -k client.keytab client/localhost@ALLUXIO.COM
kadmin.local: xst -norandkey -k foo.keytab foo/localhost@ALLUXIO.COM
```

Third, exit the console by `CTRL + D` or typing `quit` in the kadmin shell, and validate that the keytab files are correctly generated:

```bash
$ sudo klist -k -t -e alluxio.keytab
```

You should see a list of encrypted credentials for principal `alluxio/localhost@ALLUXIO.COM`
You can also do `kinit` to ensure the principal can be logged-in with those keytab files:

```bash
$ sudo kinit -k -t alluxio.keytab alluxio/localhost@ALLUXIO.COM
```

Then `sudo klist` should show the login user is `alluxio/localhost@ALLUXIO.COM`, with expiration date.
`sudo kdestroy` will logout the current Kerberos user.

If you are unable to `kinit` or `klist` with the keytab files, please double check the commands
and principals, re-generate the keytab files until they pass the above sanity checks. Invalid keytab
files are usually the reason for Kerberos authentication failures.

### Windows Server
On the server running Active Directory KDC, follow the steps:

First, create principals for Alluxio servers and clients either through the GUI or the command line, the principals are

```bash
alluxio/localhost@ALLUXIO.COM
client/localhost@ALLUXIO.COM
foo/localhost@ALLUXIO.COM
```

Second, create keytab files for those principals in PowerShell

```bash
$ ktpass /princ alluxio/localhost@ALLUXIO.COM /mapuser alluxio /pass <password> /out alluxio.keytab /crypto all /ptype KRB5_NT_PRINCIPAL /mapop set
$ ktpass /princ client/localhost@ALLUXIO.COM /mapuser client /pass <password> /out client.keytab /crypto all /ptype KRB5_NT_PRINCIPAL /mapop set
$ ktpass /princ foo/localhost@ALLUXIO.COM /mapuser foo /pass <password> /out foo.keytab /crypto all /ptype KRB5_NT_PRINCIPAL /mapop set
```


## Setup client-side Kerberos on Alluxio cluster

Please set up a standalone KDC before doing this.
The KDC plays the role of a server, providing authentication service to clients.
All the other nodes that contact the KDC for authentication are considered *clients*.
In a Kerberized Alluxio cluster, all the Alluxio nodes need to contact the KDC as clients.
Therefore, follow this [guide](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/Configuring_a_Kerberos_5_Client.html)
to set up the Kerberos client-side packages and configurations in each node in the Alluxio cluster (not the KDC node). Kerberos clients also need a
`/etc/krb5.conf` to communicate with the KDC.
The Kerberos client settings also work if you want to set up local Alluxio cluster on Max OS X.

Here is a sample `/etc/krb5.conf` on an Alluxio node:

### Linux

```properties
[logging]
 default = FILE:/var/log/krb5libs.log
 kdc = FILE:/var/log/krb5kdc.log
 admin_server = FILE:/var/log/kadmind.log

[libdefaults]
 default_realm = ALLUXIO.COM
 ticket_lifetime = 24h
 renew_lifetime = 7d
 forwardable = true
 rdns = false
 dns_lookup_kdc = true
 dns_lookup_realm = true
 # Optional in case “unsupported key type found the default TGT: 18” happens.
 default_tgs_enctypes = des3-hmac-sha1 des-cbc-crc des-cbc-md5
 default_tkt_enctypes = des3-hmac-sha1 des-cbc-crc des-cbc-md5
 permitted_enctypes = des3-hmac-sha1 des-cbc-crc des-cbc-md5

[realms]
ALLUXIO.COM = {
 kdc = <KDC public IP or DNS address>
 admin_server = <KDC public IP or DNS address>
}

[domain_realm]
 .alluxio.com = ALLUXIO.COM
 alluxio.com = ALLUXIO.COM
```

### Active Directory

The main differences from the Linux `krb5.conf` are the fields related to encryption type, by default, Active Directory uses encryption type `rc4-hmac`.

```properties
[logging]
 default = FILE:/var/log/krb5libs.log
 kdc = FILE:/var/log/krb5kdc.log
 admin_server = FILE:/var/log/kadmind.log

[libdefaults]
 default_realm = ALLUXIO.COM
 ticket_lifetime = 24h
 renew_lifetime = 7d
 forwardable = true
 rdns = false
 dns_lookup_kdc = true
 dns_lookup_realm = true
 # Optional in case “unsupported key type found the default TGT: 18” happens.
 default_tgs_enctypes = rc4-hmac des-cbc-crc des-cbc-md5
 default_tkt_enctypes = rc4-hmac des-cbc-crc des-cbc-md5
 permitted_enctypes = rc4-hmac des-cbc-crc des-cbc-md5

[realms]
ALLUXIO.COM = {
 kdc = <KDC public IP or DNS address>
 admin_server = <KDC public IP or DNS address>
}

[domain_realm]
 .alluxio.com = ALLUXIO.COM
 alluxio.com = ALLUXIO.COM
```

Verify the client-side Kerberos configurations by running `klist` and `kinit` commands.

## Setup Alluxio Cluster with Kerberos Security

Create user `alluxio`, `client` and `foo` on the machines that you will install Alluxio on. The user `alluxio` corresponds to the Kerberos principal
`alluxio/localhost@ALLUXIO.COM`, `client` corresponds to `client/localhost@ALLUXIO.COM`, and `foo` corresponds to `foo/localhost@ALLUXIO.COM`.
The user `alluxio` will be the Alluxio service user that starts, manages and stops Alluxio servers. This user does not have be called `alluxio` on your own deployment, and it can be an arbitrary string as long as it complies with the naming rules of the underlying operating system.

```bash
$ sudo adduser alluxio
$ sudo adduser client
$ sudo adduser foo
$ sudo passwd alluxio
$ sudo passwd client
$ sudo passwd foo
```

Alluxio server processes, e.g. *masters*, *workers*, etc.  will be running under User `alluxio`, so please add
`alluxio` to `sudoers` so that the user will have permission to access ramdisks.

Add the following lines to the end of `/etc/sudoers` (or use `visudo` as root)

```
# User privilege specification
alluxio ALL=(ALL) NOPASSWD:ALL
```

Then, distribute the server and client keytab files from KDC to **each node** of the Alluxio cluster.
Save them in some secure place and configure the user and group permission coordinately, the following snippets save
the keytab files into `/etc/alluxio/conf`, create the directory on each Alluxio node if it does not exist.

```bash
$ scp -i ~/your_aws_key_pair.pem <KDC_DNS_NAME>:alluxio.keytab /etc/alluxio/conf/
$ scp -i ~/your_aws_key_pair.pem <KDC_DNS_NAME>:client.keytab /etc/alluxio/conf/
$ scp -i ~/your_aws_key_pair.pem <KDC_DNS_NAME>:foo.keytab /etc/alluxio/conf/
```

```bash
$ sudo chown alluxio:alluxio /etc/alluxio/conf/alluxio.keytab
$ sudo chown client:alluxio /etc/alluxio/conf/client.keytab
$ sudo chown foo:alluxio /etc/alluxio/conf/foo.keytab

$ sudo chmod 0440 /etc/alluxio/conf/alluxio.keytab
$ sudo chmod 0440 /etc/alluxio/conf/client.keytab
$ sudo chmod 0440 /etc/alluxio/conf/foo.keytab
```

The owner of each keytab file should be the user who needs to access it.

To transfer files from Windows to Linux, you can use `scp` through [Cygwin](https://www.cygwin.com/), or use `pscp.exe` in [PuTTY](http://www.chiark.greenend.org.uk/~sgtatham/putty/latest.html).

### Server Configuration

Login as `alluxio` by executing the following:
```bash
$ su - alluxio
```

All the operations required for the rest of server configuration should be performed by user `alluxio`.

When installing Alluxio, you can add
the following configuration properties to alluxio-site.properties.
```properties
alluxio.security.authentication.type=KERBEROS
alluxio.security.authorization.permission.enabled=true
alluxio.security.kerberos.service.name=alluxio
alluxio.security.kerberos.unified.instance.name=localhost
alluxio.security.kerberos.server.principal=alluxio/localhost@ALLUXIO.COM
alluxio.security.kerberos.server.keytab.file=/etc/alluxio/conf/alluxio.keytab
```

Note:

- `alluxio.security.kerberos.service.name` is required to specify the Alluxio Service Principal
service name, and must match with the `<primary>` part of the server principal
`<primary>/<instance>@REALM.COM`.
- `alluxio.security.kerberos.unified.instance.name` is optional when all the Alluxio servers
share a single principal and a unified instance name. If this is not specified, the
`alluxio.security.kerberos.server.principal` must have the `<instance>` name matching with the
serve hostname, i.e. `alluxio.master.hostname` or `alluxio.worker.hostname`.


Once the installation and configuration complete, start Alluxio service by executing the following:

```bash
$ ./bin/alluxio format
$ ./bin/alluxio-start.sh local SudoMount
```

### Client Configuration
Client-side access to Alluxio cluster requires the following configurations:
(Note: Server keytab file is not required for the client. The keytab files
permission are configured in a way that client users would not be able to access
server keytab file.)

```properties
alluxio.security.authentication.type=KERBEROS
alluxio.security.authorization.permission.enabled=true
alluxio.security.kerberos.service.name=alluxio
alluxio.security.kerberos.unified.instance.name=localhost
alluxio.security.kerberos.client.principal=client/localhost@ALLUXIO.COM
alluxio.security.kerberos.client.keytab.file=/etc/alluxio/conf/client.keytab
```

You can switch users by changing the client principal and keytab pair.
An alternative client Kerberos login option is to invoke `kinit` on client machines.

```
kinit -k -t /etc/alluxio/conf/client.keytab client/localhost@ALLUXIO.COM
```

Invalid principal/keytab combinations and failure to find valid Kerberos credential in the ticket
cache will result in the following error message. It indicates that the user cannot log in via
Kerberos.

```
Failed to login: <detailed reason>
```

Please see the FAQ section for more details about login failures.

### Run Sample Tests

After Alluxio is configured and installed, you can run a simple tests which will write several files to Alluxio and the configured UFS.

```bash
$ ./bin/alluxio runTests
```

## Example

You can play with the following examples to verify that the Alluxio cluster you set up is indeed
Kerberos-enabled.

First, act as super user `alluxio` by setting the following configurations in `conf/alluxio-site.properties`:

```properties
alluxio.security.kerberos.client.principal=alluxio/localhost@ALLUXIO.COM
alluxio.security.kerberos.client.keytab.file=/etc/alluxio/conf/alluxio.keytab
```

Create some directories for different users via Alluxio filesystem shell:

```bash
$ ./bin/alluxio fs ls /
$ ./bin/alluxio fs mkdir /admin
$ ./bin/alluxio fs mkdir /client
$ ./bin/alluxio fs chown client /client
$ ./bin/alluxio fs chgrp client /client
$ ./bin/alluxio fs mkdir /foo
$ ./bin/alluxio fs chown foo /foo
$ ./bin/alluxio fs chgrp foo /foo
```

Now, you have `/admin` owned by user `alluxio`, `/client` owned by user `client`, and `/foo` owned by user `foo`.

If you change one or both of the above configurations to empty or a wrong value, then the
Kerberos authentication
should fail, so any command in `./bin/alluxio fs` should fail too.

Second, act as user `client` by re-configuring `conf/alluxio-site.properties`:

```properties
alluxio.security.kerberos.client.principal=client/localhost@ALLUXIO.COM
alluxio.security.kerberos.client.keytab.file=/etc/alluxio/conf/client.keytab
```

Create some directories and put some files into Alluxio:

```bash
$ ./bin/alluxio fs ls -R /
$ ./bin/alluxio fs mkdir /client/dir
$ ./bin/alluxio fs copyFromLocal conf/alluxio-site.properties /client/file
$ ./bin/alluxio fs rm -R /client/dir
$ ./bin/alluxio fs mkdir /foo/bar
$ ./bin/alluxio fs rm -R /foo
```

The last two commands should fail since user `client` has no write permission to `/foo` which is owned by user `foo`.

Similarly, switch to user `foo` and try the filesystem shell:

```properties
alluxio.security.kerberos.client.principal=foo/localhost@ALLUXIO.COM
alluxio.security.kerberos.client.keytab.file=/etc/alluxio/conf/foo.keytab
```

```bash
$ ./bin/alluxio fs ls -R /
$ ./bin/alluxio fs mkdir /foo/bar
$ ./bin/alluxio fs copyFromLocal conf/alluxio-site.properties /foo/bar/testfile
$ ./bin/alluxio fs copyFromLocal conf/alluxio-site.properties /client/foofile
```

The last command should fail because user `foo` has no write permission to `/client` which is owned by user `client`.

Alternatively, if the [kerberos credential cache](https://web.mit.edu/kerberos/krb5-1.12/doc/basic/ccache_def.html) is of type `DIR` or `FILE`, the client can login through loading the credentials from the cache instead of the keytab file

```properties
alluxio.security.kerberos.client.principal=client/localhost@ALLUXIO.COM
alluxio.security.kerberos.client.keytab.file=
```

```bash
$ kinit -k -t /etc/alluxio/conf/client.keytab client/localhost@ALLUXIO.COM
```

This would have the same affect as setting up the client keytab files.
You can validate this by running similar examples as above:

```bash
$ ./bin/alluxio fs ls -R /
$ ./bin/alluxio fs mkdir /client/dir
$ ./bin/alluxio fs copyFromLocal conf/alluxio-site.properties /client/file
$ ./bin/alluxio fs rm -R /client/dir
$ ./bin/alluxio fs mkdir /foo/bar
$ ./bin/alluxio fs rm -R /foo
```

## Using Delegation Token

When Kerberoized Alluxio is used with a Kerberoized Hadoop cluster, Alluxio can be configured to
use delegation token instead of client principals and keytabs on compute nodes. Using delegation token
reduces workload on KDC by greatly reducing the number of requests to KDC when a compute job is started.
It also removes the requirement of having to deploy a client keytab to all compute node, thus makes it
easier to deploy and maintain Alluxio clients. It is recommended to use delegation token whenever possible.

To enable delegation token on Alluxio, first configure the compute frameworks to obtain delegation tokens
from Alluxio.

First, please add Alluxio client jar location to YARN resource manager class path:
```properties
export HADOOP_CLASSPATH=<PATH_TO_ALLUXIO_CLIENT_JAR>:${HADOOP_CLASSPATH}
```
Replace `<PATH_TO_ALLUXIO_CLIENT_JAR>` with the actual Alluxio client jar location on the
YARN resource manager node. After the change, please restart the resource manager.

For Spark, please add the following property to spark-defaults.conf and restart Spark and YARN:
```properties
spark.yarn.access.hadoopFileSystems=<ALLUXIO_ROOT_URL>
```
Replace `<ALLUXIO_ROOT_URL>` with the actual Alluxio URL starting with `alluxio://`.
In single master mode, this URL can be `alluxio://<HOSTNAME>:<PORT>/`. In HA mode,
this URL should be `alluxio://<ALLUXIO_SERVICE_ALIAS>/`.

For map reduce, please add the following property and restart YARN:
```properties
mapreduce.job.hdfs-servers=<ALLUXIO_ROOT_URL>
```
Replace `<ALLUXIO_ROOT_URL>` with the actual Alluxio URL starting with `alluxio://`.

In order to eliminate the requirement of client keytab on compute nodes, capability should also be
enabled on Alluxio cluster. Please set the following property in `alluxio-site.properties`
on all Alluxio nodes:
```properties
alluxio.security.authorization.capability.enabled=true
```
Also make sure the client keytab and principal are **not set** in the client and server configuration:
```properties
alluxio.security.kerberos.client.principal=<CLIENT_PRINCIPAL>
alluxio.security.kerberos.client.keytab.file=<CLIENT_KEYTAB>
```
Please restart Alluxio and corresponding compute framework clients after the configuration change.

## Kerberos-enabled Alluxio Integration with Secure-HDFS as UFS
If there is an existing Secure-HDFS with Kerberos enabled, here are the instructions to set up
Alluxio to leverage the Secure-HDFS as the UFS.

In order to mount a secure HDFS to Alluxio, you will need a kerberos principal and keytab file for
an HDFS user. This HDFS user must either be a superuser for HDFS, or be able to impersonate other
HDFS users.

In order for an HDFS user to be a superuser, the user must be in the OS group on the namenode,
specified by the Hadoop configuration: `dfs.permissions.superusergroup`.

In order to enable an HDFS user to impersonate other HDFS users, additional Hadoop configuration
is required. To enable impersonation for an HDFS user named `alluxiohdfs`, the following HDFS
configuration parameters need to be set in `core-site.xml` and HDFS must be restarted:

```xml
<property>
  <name>hadoop.proxyuser.alluxiohdfs.hosts</name>
  <value>*</value>
</property>
<property>
  <name>hadoop.proxyuser.alluxiohdfs.groups</name>
  <value>*</value>
</property>
```

Once HDFS is configured for the `alluxiohdfs` user, and the kerberos keytab is generated for the
principal, the keytab must be distributed to all of the Alluxio servers (workers and masters). Now,
Alluxio is ready to mount a secure HDFS. There are two ways to mount a secure HDFS to Alluxio; a
root mount, or a nested mount.

### Secure HDFS as a root mount
To configure Alluxio to root mount a secure HDFS, several configuration parameters are necessary
in `alluxio-site.properties`:

```properties
alluxio.underfs.address=hdfs://<ADDRESS>/<PATH>/
alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.version=<HDFS_VERSION>
alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration=core-site.xml:hdfs-site.xml
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.principal=alluxiohdfs@ALLUXIO.COM
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.keytab.file=/alluxio/alluxiohdfs.keytab
alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.impersonation.enabled=true|false
```

- `alluxio.underfs.address`: this specifies the URI to the HDFS to mount
- `alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.version`: This species the version of HDFS for this mount point. This [page lists the supported HDFS versions](Multiple-HDFS.html#supported-hdfs-versions).
- `alluxio.master.mount.table.root.option.alluxio.underfs.hdfs.configuration`: This points to a `:` separated list of files that define the HDFS configuration. Typically, this should point to the `core-site.xml` file and the `hdfs-site.xml` file.
- `alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.principal`: Specfies the principal name to connect to this HDFS. In this example, it is `alluxiohdfs@ALLUXIO.COM`
- `alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.kerberos.client.keytab.file`: Specifies the location of the keytab file for the principal. This location must be the same on all the masters and workers.
- `alluxio.master.mount.table.root.option.alluxio.security.underfs.hdfs.impersonation.enabled`: If true, this means Alluxio should connect to the HDFS cluster using impersonation. If false, Alluxio will interact with the HDFS cluster directly with the previously specified principal.

Once these parameters are configured, Alluxio will have the secure HDFS cluster mounted at the root.

### Secure HDFS as a nested mount
Alluxio can also mount an secure HDFS as a nested mount (not the root mount). To configure Alluxio
in this scenario is very similar to the the root mount scenario, except the configuration is
specified in the mount command, and not the configuration file. The following Alluxio CLI command
will mount a secure HDFS as a nested mount:

```bash
$ ./bin/alluxio fs mount --option alluxio.underfs.hdfs.version=<HDFS_VERSION> \
  --option alluxio.underfs.hdfs.configuration=core-site.xml:hdfs-site.xml \
  --option alluxio.security.underfs.hdfs.kerberos.client.principal=alluxiohdfs@ALLUXIO.COM \
  --option alluxio.security.underfs.hdfs.kerberos.client.keytab.file=/alluxio/alluxiohdfs.keytab \
  --option alluxio.security.underfs.hdfs.impersonation.enabled=true|false \
  /mnt/secure-hdfs/ hdfs://<ADDRESS>/<PATH>/
```

The descriptions of the parameters are described earlier.

## Running Spark with Kerberos-enabled Alluxio and Secure-HDFS
Follow the [Running-Spark-on-Alluxio](Running-Spark-on-Alluxio.html) guide to set up
`SPARK_CLASSPATH`. In addition, the following items should be added to make Spark aware of Kerberos
configuration:

- You can **only** use Spark on a Kerberos-enabled cluster in the YARN mode, not in the
Standalone mode. Therefore, a secure YARN must be set up first.

- Copy hadoop configurations (usually in `/etc/hadoop/conf/`) `hdfs-site.xml`,
`core-site.xml`, `yarn-site.xml` to `{SPARK_HOME}/conf`.

- Copy Alluxio site configuration `{ALLUXIO_HOME}/conf/alluxio-site.properties` to
`{SPARK_HOME}/conf` for Spark to pick up Alluxio configurations such as Kerberos related flags.

- When launching Spark shell or jobs, please add `--principal` and `--keytab` to specify Kerberos
principal and keytab files for Spark.

```
./bin/spark-shell --principal=alluxio/localhost@ALLUXIO.COM --keytab=/etc/alluxio/conf/alluxio.keytab
```

## Running Flink with Kerberos-enabled Alluxio and Secure-HDFS
Follow the [Running-Flink-on-Alluxio](Running-Flink-on-Alluxio.html) guide to set up
`conf/flink-conf.yaml`. In addition, the following items should be added:

- Please add the following property to HDFS core-site.xml, to use `alluxio://` prefix.

```xml
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
```

- In “conf/flink-conf.yaml”, set  `fs.hdfs.hadoopconf` to hadoop configuration **directory**
(usually `/etc/hadoop/conf/`).

```properties
fs.hdfs.hadoopconf: /etc/hadoop/conf/
```

- Translate Alluxio site configuration (`{ALLUXIO_HOME}/conf/alluxio-site.properties`) to
`env.java.opts` in `{FLINK_HOME}/conf/flink-conf.yaml` for Flink to pick up Alluxio configurations
such as Kerberos related flags.

```properties
env.java.opts: "-Dalluxio.security.authentication.type=KERBEROS -Dalluxio.security.authorization.permission.enabled=true -Dalluxio.security.kerberos.server.principal=alluxio/localhost@ALLUXIO.COM -Dalluxio.security.kerberos.server.keytab.file=/etc/alluxio/conf/alluxio.keytab -Dalluxio.security.kerberos.client.principal= -Dalluxio.security.kerberos.client.keytab.file="
```

- If you see the following failure, it's because that hadoop client configuration is not picked up by Flink correctly. Try `export HADOOP_CONF_DIR=<YOUR_HADOOP_CONF_DIR>`

```
org.apache.hadoop.security.AccessControlException: SIMPLE authentication is not enabled.  Available:[TOKEN, KERBEROS]
```

## FAQ

Java Kerberos error messages can be hard to interpret. In general, it is helpful to enable Kerberos
debug messages by adding the following to the JVM.

```
-Dsun.security.krb5.debug=true
```

### Unable to obtain password from user
This is typically because the keytab file is invalid, e.g. with wrong principal name, or not set with
right permission for alluxio service to access. Most of the time, it is because the credential is not
valid. Please double check the existence and permission of the keytab file, or the `klist` result.

Alternatively, the KDC log may indicate if any KDC requests are actually sent to KDC.

### GSS initiate failed

Type 1: `No Valid Credentials Provided (mechanism Level: Failed to Find Any Kerberos TGT)`

This error means the user is NOT authenticated.

Possible causes:

- Your process was issued with a ticket, which has now expired.
- You did specify a keytab but it isn't there or is somehow otherwise invalid
- You don't have the Java Cryptography Extensions installed.
- The principal isn't in the same realm as the service, so a matching TGT cannot be found. That is: you have a TGT, it's just for the wrong realm.


Type 2: `No reason specified.`

This is a bit harder to troubleshoot. If no obvious reason for the authentication failure root cause,
it is still very likely to be the same causes as "Failed to Find Any Kerberos TGT). Always sanity check the user’s setup
environment, make sure all the Kerberos credentials are valid. You can try to enable
`-Dsun.security.krb5.debug=true`to find out more details about why Kerberos authentication failed.

### Encryption type AES256 CTS mode with HMAC SHA1-96 is not supported/enabled

Possible reasons:

- JVM doesn't have the JCE JAR installed.
- Before Alluxio service was started with Kerberos, there were some other Kerberos ticket cached.

### SIMPLE authentication is not enabled. Available:[TOKEN, KERBEROS]

This error is from Secure-HDFS. When Alluxio uses HDFS as the UFS, all the HDFS operations are
delegated to Alluxio servers. Alluxio server will use Hadoop client to communicate with
Kerberized-HDFS. It is required to set the right Hadoop authentication type in Hadoop client
`core-site.xml`.

Please double check in `{ALLUXIO_HOME}/conf/` (or the customized `Configuration.UNDERFS_HDFS_CONFIGURATION`)
directory, if the `core-site.xml` does not exist, please copy the right `core-site` and `hdfs-site`
here. If `core-site.xml` already exists, open the file and double check the property called
`hadoop.security.authentication`. Make sure it is set to `KERBEROS`.

### Permission not in sync with HDFS

Note that if the permission is updated in HDFS bypassing Alluxio, Alluxio will NOT automatically
sync those changes in UFS to alluxio namespace. User can use `loadmetadata -f` to force reload the metadata from HDFS.

### kinit -R failures

kinit: Ticket expired while renewing credentials

- Solution 1: Check `max_renewable_life` in kdc.conf, set it to a positive value (say 10d) and restart KDC and kadmin services. Retry `kinit -R`
- Solution 2

```bash
$ modprinc -maxrenewlife 10days krbtgt/<host>@<realm>
```

### Clock Skew Too Great
This comes from the clocks on the machines being too far out of sync.
If it's a physical cluster, make sure that your NTP daemons are pointing at the same NTP server,
one that is actually reachable from the Hadoop cluster.
And that the timezone settings of all the hosts are consistent.

### Request Is a Replay
A replay cache (or "rcache") keeps track of all authenticators recently presented to a service.
The following error message usually indicates a duplicate authentication request is detected in the replay cache.

```
ERROR transport.TSaslTransport (TSaslTransport.java:open) - SASL negotiation failure
javax.security.sasl.SaslException: GSS initiate failed [Caused by GSSException: Failure unspecified at GSS-API level (Mechanism level: Request is a replay (34))]
    at com.sun.security.sasl.gsskerb.GssKrb5Server.evaluateResponse(GssKrb5Server.java:176)
    at org.apache.thrift.transport.TSaslTransport$SaslParticipant.evaluateChallengeOrResponse(TSaslTransport.java:539)
    at org.apache.thrift.transport.TSaslTransport.open(TSaslTransport.java:283)
```

Potential causes are

- Clocks across nodes are out of sync. Please make sure all nodes run NTP so
that clocks are in sync.
- Alluxio servers or client principals on multiple nodes are using the same
kerberos principal, rather than per-host principal (alluxio/fully.qualified.hostname@EXAMPLE.COM).

### User <YARN_USER> is not configured for any impersonation.
This can happen when YARN is trying renew a delegation token with an impersonated user.
To solve the issue, please add the following property to alluxio-site.properties on all
Alluxio nodes and restart Alluxio:
```properties
alluxio.master.security.impersonation.<YARN_USER>.users=*
```
Replace `<YARN_USER>` with the actual YARN user name in the error message.

### User <SOME_USER> cannot renew a token with mismatched renewer <YARN_USER>
This can happen if Hadoop is configured with a custom auth-to-local rule that translates the YARN
user principal to a different local user. To solve this issue, configure Alluxio to use auth-to-local
rule which translates the YARN principal to the same local user as the Hadoop cluster does.

For example, if you encounter an error message `User rm cannot renew a token with mismatched renewer yarn`,
and found out that Hadoop cluster has the following auth-to-local property in `core-site.xml`:

```xml
    <property>
      <name>hadoop.security.auth_to_local</name>
      <value>
      ...
      RULE:[2:$1@$0](rm@ALLUXIO.COM)s/.*/yarn/
      ...
      DEFAULT
      </value>
    </property>
```

You can fix the issue by adding the following property to `alluxio-site.properties` on all
Alluxio nodes:
```properties
alluxio.security.kerberos.auth.to.local=RULE:[2:$1@$0](rm@ALLUXIO.COM)s/.*/yarn/ DEFAULT
```
Please restart Alluxio cluster after making the change.

### Hive/Tez job nodes failed with error "DIGEST-MD5: digest response format violation."
This issue can happen when running a Tez job longer than the delegation token expiration
interval (the default value is 24 hours). Tez does not automatically renew a delegation
token for files used by a job. Therefore once a job runs longer than the interval the token
will expire.

To workaround this limitation, please set the following property in `tez-site.xml`:
```xml
    <property>
      <name>tez.aux.uris</name>
      <value>alluxio://<ALLUXIO_SERVICE_ADDRESS>/<PATH_TO_NON_EMPTY_DIR>/</value>
    </property>
```
Please set the value to an Alluxio location with at least one file in it. This will tell
Tez to obtain an Alluxio delegation token when a session is started and pass the credentials
to YARN resource manager for renewal. It is preferred that the URI in the value is set to
a location in Alluxio with only one empty file, to minimize the overhead of copying such
file for the job.
