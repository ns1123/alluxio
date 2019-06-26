---
layout: global
title: Security
nickname: Security
group: Advanced
priority: 0
---

* Table of Contents
{:toc}

This document describes the following security related features in Alluxio.

1. [User Authentication](#authentication): 
Alluxio filesystem will differentiate users accessing the service
when the authentication mode is `SIMPLE`.
<!-- ALLUXIO CS REPLACE -->
<!-- Alluxio also supports `NOSASL` mode which ignores authentication. -->
<!-- Authentication mode `SIMPLE` is required to enable authorization. -->
<!-- ALLUXIO CS WITH -->
Alluxio also supports `NOSASL` and `KERBEROS` as authentication mechanisms.
Having authentication mode to be `SIMPLE`, or `KERBEROS` is required for authorization.
<!-- ALLUXIO CS END -->
1. [User Authorization](#authorization): 
Alluxio filesystem will grant or deny user access based on the requesting user and
the POSIX permissions model of the files or directories to access,
when `alluxio.security.authorization.permission.enabled=true`.
Note that, authentication cannot be `NOSASL` as authorization requires user information.
1. [Access Control Lists](#access-control-lists): In addition to the POSIX permission model, Alluxio
implements an Access Control List (ACL) model similar to those found in Linux and HDFS. The ACL model
is more flexible and allows administrators to manage any user or group's permissions to any file
system object.
1. [Client-Side Hadoop Impersonation](#client-side-hadoop-impersonation): Alluxio supports
client-side Hadoop impersonation so the Alluxio client can access Alluxio on the behalf of the
Hadoop user. This can be useful if the Alluxio client is part of an existing Hadoop service.
1. [Auditing](#auditing): If enabled, the Alluxio filesystem
writes an audit log for all user accesses.
<!-- ALLUXIO CS ADD -->
1. [Encryption](#encryption): Alluxio supports end-to-end data encryption, and TLS for network communication.
<!-- ALLUXIO CS END -->

See [Security specific configuration]({{ '/en/reference/Properties-List.html' | relativize_url }}#security-configuration)
for different security properties.

## Authentication

The authentication protocol is determined by the configuration property `alluxio.security.authentication.type`,
with a default value of `SIMPLE`.

### SIMPLE

Authentication is **enabled** when the authentication type is `SIMPLE`.

A client must identify itself with a username to the Alluxio service.
If the property `alluxio.security.login.username` is set on the Alluxio client, its value will be
used as the login user, otherwise, the login user is inferred from the operating system user
executing the client process.
The provided user information is attached to the corresponding metadata when the client creates
directories or files.

### NOSASL

Authentication is **disabled** when the authentication type is `NOSASL`.

The Alluxio service will ignore the user of the client and no user information will be attached to the
corresponding metadata when the client creates directories or files. 

### CUSTOM

Authentication is **enabled** when the authentication type is `CUSTOM`.

Alluxio clients retrieves user information via the class provided by the
`alluxio.security.authentication.custom.provider.class` property.
The specified class must implement the interface `alluxio.security.authentication.AuthenticationProvider`.

This mode is currently experimental and should only be used in tests.

<!-- ALLUXIO CS ADD -->
### KERBEROS

Authentication is enabled and enforced via Kerberos. Kerberos is an authentication protocol that
provides strong and mutual authentication between clients and servers.

The typical Kerberos principal format used for services is `"primary/instance@REALM.COM"`.
It is required to prepare the Kerberos principals as Alluxio service principal names (SPN). The
primary part of the Kerberos service principal must match with the service name defined in the
following Alluxio configuration property:

```
alluxio.security.kerberos.service.name=<alluxio-service-name>
```

It is recommended to use hostname-associated instance name of Alluxio service principals, such as:
`<alluxio-service-name>/<hostname>@REALM.COM`. This way each Alluxio server node has a unique
service principal. Note that the `<hostname>` in each principal must match the server (either master
or worker) hostname.

On the other hand, Alluxio also supports cluster-wide unified instance name, like
`<alluxio-service-name>/<alluxio-cluster-name>@REALM.COM`, so that all the Alluxio servers
share the same principal. To use this feature, please set
`alluxio.security.kerberos.unified.instance.name=<alluxio-cluster-name>`.

Alluxio Enterprise Edition supports two ways to setup Kerberos authentication:

1. Java Kerberos
2. MIT native Kerberos (through JGSS)

#### Java Kerberos

Please refer to [Kerberos security setup](Kerberos-Security-Setup.html) instructions to set
set up Alluxio with Java Kerberos enabled.

Alluxio system administrators are responsible for specifying the Alluxio servers Kerberos
credentials, with principal name and keytab file. Alluxio clients need valid Kerberos credentials
(either keytab files or local ticket cache) to access a Kerberos-enabled Alluxio cluster.

When KERBEROS authentication is enabled, the login user for different component is obtained as follows:

1. For Alluxio servers, the login user is represented by the server-side Kerberos principal in
`alluxio.security.kerberos.server.principal`.
A corresponding keytab file must be specified in `alluxio.security.kerberos.server.keytab.file`.
The Alluxio user shown in Alluxio namespace is the short
name of the Kerberos principal, which excludes the hostname and realm part.

2. For Alluxio clients, the login user is represented by the client-side Kerberos principal in
`alluxio.security.kerberos.client.principal`.  There are two ways for the Alluxio clients to login via Kerberos.
One is specify a keytab file in `alluxio.security.kerberos.client.keytab.file`.
The other way is to do `kinit` Kerberos login for the `alluxio.security.kerberos.client.principal` name on
the client machine. Alluxio client first checks whether there is a valid `alluxio.security.kerberos.client.keytab.file`.
If there is no valid keytab file which can login successfully, Alluxio client will fall back to find
the login info in the ticket cache. If none of those Kerberos credentials exist, Alluxio client will throw a
login failure, and ask the user to provide the keytab file, or login via `kinit`.

#### MIT Kerberos

Please refer to [MIT Kerberos security setup](Native-Kerberos-Security-Setup.html) for detailed
instructions to setup Alluxio with MIT native Kerberos.

The default Java GSS implementation relies on JAAS KerberosLoginModule for initial credential
acquisition. In contrast, when native platform Kerberos integration is enabled, the
initial credential acquisition should happen prior to calling JGSS APIs, e.g. through kinit.
When enabled, Java GSS would look for native GSS library using the operating system specific name,
e.g. Solaris: libgss.so vs Linux: libgssapi.so. If the desired GSS library has a different name
or is not located under a directory for system libraries, then its full path should be specified
using the system property `sun.security.jgss.lib`.

#### Connecting with Secure-HDFS

Note that in Alluxio Enterprise edition the way to configure Alluxio with secure-HDFS is different from
that in Alluxio enterprise edition. `alluxio.master.keytab.file`, `alluxio.master.principal`,
`alluxio.worker.keytab.file` and `alluxio.worker.principal` are deprecated. Please use
`alluxio.security.underfs.hdfs.kerberos.client.keytab.file` and
`alluxio.security.underfs.hdfs.kerberos.client.principal` instead. Note that Alluxio can use a different
principal other than Alluxio server principal to access secure HDFS. If
`alluxio.security.underfs.hdfs.kerberos.client.principal` is not specified, then Alluxio falls back to
using `alluxio.security.kerberos.server.principal`.

If you want to setup a Kerberos-enabled Alluxio cluster on top of Kerberos-enabled HDFS, please
refer to "Kerberos-enabled Alluxio integration with Secure-HDFS" in [Kerberos setup guide](Kerberos-Security-Setup.html)
or [MIT Kerberos security setup](Native-Kerberos-Security-Setup.html) for more details.

#### Auth-to-local configuration
Alluxio supports configurable translation from Kerberos principal name to operating system user,
via MIT Kerberos [auth_to_local](https://web.mit.edu/kerberos/krb5-1.12/doc/admin/conf_files/krb5_conf.html)
conf. To make it easier to configure together with HDFS, the syntax
is the same as Hadoop [auth_to_local](https://www.cloudera.com/documentation/enterprise/5-3-x/topics/cdh_sg_kerbprin_to_sn.html).
Please use the `alluxio.security.kerberos.auth.to.local` configuration property to set it up.
By default the value is `DEFAULT`.

<!-- ALLUXIO CS END -->
## Authorization

The Alluxio filesystem implements a permissions model similar to the POSIX permissions model.

Each file and directory is associated with:

- An owner, which is the user of the client process to create the file or directory.
- A group, which is the group fetched from user-groups-mapping service. See [User group mapping](#user-group-mapping).
- Permissions, which consist of three parts:
  - Owner permission defines the access privileges of the file owner
  - Group permission defines the access privileges of the owning group
  - Other permission defines the access privileges of all users that are not in any of above two classes

Each permission has three actions:

1. read (r)
2. write (w)
3. execute (x)

For files:
- Read permissions are required to read files
- Write permission are required to write files

For directories:
- Read permissions are required to list its contents
- Write permissions are required to create, rename, or delete files or directories under it
- Execute permissions are required to access a child of the directory

The output of the `ls` shell command when authorization is enabled is:

```bash
./bin/alluxio fs ls /
drwxr-xr-x jack           staff                       24       PERSISTED 06-14-2019 07:02:45:248  DIR /default_tests_files
-rw-r--r-- jack           staff                       80   NOT_PERSISTED 06-14-2019 07:02:26:487 100% /default_tests_files/BASIC_CACHE_PROMOTE_MUST_CACHE
```

### User group mapping

For a given user, the list of groups is determined by a group mapping service, configured by
the `alluxio.security.group.mapping.class` property, with a default implementation of
`alluxio.security.group.provider.ShellBasedUnixGroupsMapping`.
This implementation executes the `groups` shell command on the local machine
to fetch the group memberships of a particular user.
Running the `groups` command for every query may be expensive, so
the user group mapping is cached, with an expiration period configured by the
`alluxio.security.group.mapping.cache.timeout` property, with a default value of `60s`.
If set to a value of `0`, the caching is disabled.
If the cache timeout is too low or disabled, the `groups` command will be run very frequently, but
may increase latency for operations.
If the cache timeout is too high, the `groups` command will not be run frequently, but the cached
results may become stale.

Alluxio has super user, a user with special privileges typically needed to administer and maintain the system.
The super user is the operating system user executing the Alluxio master process. 
The `alluxio.security.authorization.permission.supergroup` property defines a super group.
Any additional operating system users belong to this operating system group are also super users.

<!-- ALLUXIO CS ADD -->
#### LDAP

If your organization use OpenLDAP or Active Directory to manage identities,
it is recommended to sync LDAP users and groups to machines' operating system running Alluxio.
Alternatively, Alluxio also supports direct connection to OpenLDAP or Active Directory for group mapping service.
To use the Alluxio integration with OpenLDAP or Active Directory, set the following properties in `alluxio-site.properties`.

<table class="table table-striped">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
{% for item in site.data.table.ldap-configuration-EE %}
  <tr>
    <td class="td-property">{{ item.propertyName }}</td>
    <td class="td-default">{{ item.defaultValue }}</td>
    <td class="td-meaning">{{ site.data.table.en.ldap-configuration-EE[item.propertyName] }}</td>
  </tr>
{% endfor %}
</table>

Example configuration for an LDAP server without SSL:

```properties
alluxio.security.group.mapping.class=alluxio.security.group.provider.LdapGroupsMapping
alluxio.security.group.mapping.ldap.url=ldap://example.com:389
alluxio.security.group.mapping.ldap.base=cn=Users,dc=example,dc=com
alluxio.security.group.mapping.ldap.bind.user=cn=alluxio,cn=Users,dc=example,dc=com
alluxio.security.group.mapping.ldap.bind.password=secret
```

Example configuration for an LDAP server with SSL:

```properties
alluxio.security.group.mapping.class=alluxio.security.group.provider.LdapGroupsMapping
alluxio.security.group.mapping.ldap.url=ldaps://example.com:636
alluxio.security.group.mapping.ldap.ssl=true
alluxio.security.group.mapping.ldap.ssl.keystore=/path/to/ldap.jks
alluxio.security.group.mapping.ldap.ssl.keystore.password=secret
alluxio.security.group.mapping.ldap.base=cn=Users,dc=example,dc=com
alluxio.security.group.mapping.ldap.bind.user=cn=alluxio,cn=Users,dc=example,dc=com
alluxio.security.group.mapping.ldap.bind.password=secret
```

If you have your own way of managing the SSL keystore, configure the properties
related to LDAP SSL keystore according to your setup.

Otherwise, here is an example for generate the SSL keystore:

```bash
# get the LDAP server's certificate by
$ echo  | openssl s_client -connect example.com:636 2>/dev/null | openssl x509 > /tmp/ldap.crt

# add the certificate to Java's trusted keystore by
$ sudo keytool -import -noprompt -trustcacerts -alias ldap -file /tmp/ldap.crt -keystore ${JAVA_HOME}/jre/lib/security/cacerts

# generate the keystore in JKS format, in the prompt, specify password as the value for property
# "alluxio.security.group.mapping.ldap.ssl.keystore.password", answer "yes" to the question of whether to trust the certificate
$ keytool -import -keystore /path/to/ldap.jks -file /tmp/ldap.crt
```

<!-- ALLUXIO CS END -->
### Initialized directory and file permissions

When a file is created, it is initially assigned fully opened permissions of `666` by default.
Similarly, a directory is initially assigned with `777` permissions.
A umask is applied on the initial permissions; this is configured by the
`alluxio.security.authorization.permission.umask` property, with a default of `022`.
Without any property modifications, files and directories are created with `644` and `755` permissions respectively.

### Update directory and file permission model

The owner, group, and permissions can be changed by two ways:

1. User application invokes the `setAttribute(...)` method of `FileSystem API` or `Hadoop API`.
2. CLI command in shell. See
[chown]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#chown),
[chgrp]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#chgrp),
[chmod]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#chmod).

The owner attribute can only be changed by a super user.
The group and permission attributes can be changed by a super user or the owner of the path.

## Access Control Lists

The POSIX permissions model allows administrators to grant permissions to owners, owning groups and other users.
The permission bits model is sufficient for most cases. 
However, to help administrators express more complicated security policies, Alluxio also supports Access Control Lists (ACLs).
ACLs allow administrators to grant permissions to any user or group.

A file or directory's Access Control List consists of multiple entries.
The two types of ACL entries are Access ACL entries and Default ACL entries. 

### 1. Access ACL Entries:

This type of ACL entry specifies a particular user or group's permission to read, write and
execute. 

Each ACL entry consists of:
- a type, which can be one of user, group or mask
- an optional name 
- a permission string similar to the POSIX permission bits

The following table shows the different types of ACL entries that can appear in the access ACL: 

|ACL Entry Type| Description|
|:----------------------:|:-----------------------|
|user:userid:permission  | Sets the access ACLs for a user. Empty userid implies the permission is for the owner of the file.|
|group:groupid:permission| Sets the access ACLs for a group. Empty groupid implies the permission is for the owning group of the file.|
|other::permission       | Sets the access ACLs for all users not specified above.|
|mask::permission        | Sets the effective rights mask.  The ACL mask indicates the maximum permissions allowed for all users other than the owner and for groups.|

Notice that ACL entries describing owner's, owning group's and other's permissions already exist in
the standard POSIX permission bits model.
For example, a standard POSIX permission of `755` translates into an ACL list as follows:
```text
user::rwx
group::r-x
other::r-x
```

These three entries are always present in each file and directory.
When there are entries in addition to these standard entries, the ACL is considered an extended ACL. 

A mask entry is automatically generated when an ACL becomes extended.
Unless specifically set by the user, the mask's value is adjusted to be the union of all permissions affected by the mask entry.
This includes all the user entries other than the owner and all group entries. 
	
For the ACL entry `user::rw-`:
- the type is `user`
- the name is empty, which implies the owner
- the permission string is `rw-`

This culminates to the owner has `read` and `write` permissions, but not `execute`.

For the ACL entry `group:interns:rwx` and mask `mask::r--`:
- the entry grants all permissions to the group `interns`
- the mask only allows `read` permissions

This culminates to the `interns` group having only `read` access because the mask disallows all other permissions.  

### 2. Default ACL Entries:

**Default ACLs only apply to directories.**
Any new file or directory created within a directory with a default ACL will inherit the default ACL as its access ACL. 
Any new directory created within a directory with a default ACL will also inherit the default ACL as its default ACL. 

**Default ACLs also consists of ACL entries, similar to those found in access ACLs.** 
The are distinguished by the `default` keyword as the prefix.
For example, `default:user:alluxiouser:rwx` and `default:other::r-x` are both valid default ACL entries.

Given a `documents` directory, its default ACL can be set to `default:user:alluxiouser:rwx`.
The user `alluxiouser` will have full access to any new files created in the `documents` directory.
These new files will have an access ACL entry of `user:alluxiouser:rwx`.
Note that the ACL does not grant the user `alluxiouser` any additional permissions to the directory.

### Managing ACL entries

ACLs can be managed by two ways:

1. User application invokes the `setFacl(...)` method of `FileSystem API` or `Hadoop API` to change the ACL and invokes the `getFacl(...)` to obtain the current ACL. 
2. CLI command in shell. See
[getfacl]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#getfacl)
[setfacl]({{ '/en/basic/Command-Line-Interface.html' | relativize_url }}#setfacl),

The ACL of a file or directory can only be changed by super user or its owner.

## Client-Side Hadoop Impersonation

When Alluxio is used in a Hadoop environment, a user, or identity, can be specified for both the
Hadoop client and the Alluxio client. Since the Hadoop client user and the Alluxio client user can
specified independently, the users could be different from each other. The Hadoop client user may
even be in a separate namespace from the Alluxio client user.

Alluxio client-side Hadoop impersonation solves the issues when the Hadoop client user is different
from the Alluxio client user. With this feature, the Alluxio client examines the Hadoop client user,
and then attempts to impersonate as that Hadoop client user.

For example, a Hadoop application can be configured to run as the Hadoop client user `foo`, but the
Alluxio client user is configured to be `yarn`. This means any data interactions will be attributed
to user `yarn`. With client-side Hadoop impersonation, the Alluxio client will detect the Hadoop
client user is `foo`, and then connect to Alluxio servers as user `yarn` impersonating as user
`foo`. With this impersonation, the data interactions will be attributed to user ‘foo’.

This feature is only applicable when using the hadoop compatible client to access Alluxio.

In order to configure Alluxio for client-side Hadoop impersonation, both client and server
configurations (master and worker) are required.

### Server Configuration

To enable a particular Alluxio client user to impersonate other users server (master and worker)
configuration are required.
Set the `alluxio.master.security.impersonation.<USERNAME>.users` property, where `<USERNAME>` is the name
of the Alluxio client user.

The property value is a comma-separated list of users that `<USERNAME>` is allowed to impersonate.
The wildcard value `*` can be used to indicate the user can impersonate any other user.
Some examples:

- `alluxio.master.security.impersonation.alluxio_user.users=user1,user2`
  - the Alluxio client user `alluxio_user` is allowed to impersonate `user1` and `user2`
- `alluxio.master.security.impersonation.client.users=*`
  - the Alluxio client user `client` is allowed to impersonate any user

To enable a particular user to impersonate other groups, set the
`alluxio.master.security.impersonation.<USERNAME>.groups` property, where again `<USERNAME>` is
the name of the Alluxio client user.
Similar to above, the value is a comma-separated list of groups and the wildcard value `*`
can be used to indicate all groups.
Some examples:

- `alluxio.master.security.impersonation.alluxio_user.groups=group1,group2`
  - the Alluxio client user `alluxio_user` is allowed to impersonate any users from groups `group1` and `group2`
- `alluxio.master.security.impersonation.client.groups=*`
  - the Alluxio client user `client` is allowed to impersonate users from any group

In summary, to enable an Alluxio client user to impersonate other users, at least one of the two
impersonation properties must be set on servers; setting both are allowed for the same Alluxio
client user.

### Client Configuration

After enabling impersonation on the servers for a given Alluxio client user,
the client must indicate which user it wants to impersonate.
This is configured by the `alluxio.security.login.impersonation.username` property.

If the property is set to an empty string or `_NONE_`, impersonation is disabled, and the Alluxio
client will interact with Alluxio servers as the Alluxio client user.
If the property is set to `_HDFS_USER_`, the Alluxio client will connect to Alluxio servers as the
Alluxio client user, but impersonate as the Hadoop client user when using the Hadoop compatible
client.

<!-- ALLUXIO CS ADD -->
### Data Path Authorization

In Alluxio Enterprise Edition, the access control on data transfer path (Client-Workers) is further enforced by
an enhanced distributed authorization mechanism. Alluxio worker is able to check whether the client user has the
right privilege to access the requested block, even though workers do not know about the file permission info.

This data path authorization feature is disabled by default. It can be turned on with the following configuration
`alluxio.security.authorization.capability.enabled`. When capability feature is enabled, Alluxio master verifies
the permission and grants a signed capability to the client. The capability is a token which grants the bearer
specified access rights. The capability is verified by Alluxio workers to see whether the granted permission matches
with the client’s access request. A capability is only valid for a short amount of time, which can be configured
via Alluxio server configuration `alluxio.security.authorization.capability.lifetime.ms` (default to 1 hour).

Capabilities are generated using a scheme where the Master and all Workers share a secret key, called CapabilityKey.
Only Master and Workers know the key, no third party can forge the capabilities. The capability key is generated and
rotated by Alluxio master periodically. To avoid bulk capability invalidation errors, during key rotation the old key
is still valid for a short time period to allow graceful key expiration. The workers will accept old capabilities for
a certain time period (by default 25% of the key life time) after receiving a new version of capability key.
Capability key life time can be configured via Alluxio server configuration
`alluxio.security.authorization.capability.key.lifetime.ms` (default to 1 day).

<!-- ALLUXIO CS END -->
### Exceptions

The most common impersonation error applications may see is something like
`User yarn is not configured for any impersonation. impersonationUser: foo`. This is most likely
due to the fact that the Alluxio servers have not been configured to enable impersonation for that
user. To fix this, the Alluxio servers must be configured to enable impersonation for the user in question (yarn in the example error message).

Please read this [blog post](http://www.alluxio.com/blog/alluxio-developer-tip-why-am-i-seeing-the-error-user-yarn-is-not-configured-for-any-impersonation-impersonationuser-foo) for more tips.

## Auditing

Alluxio supports audit logging to allow system administrators to track users' access to file metadata.

The audit log file at `master_audit.log` contains entries corresponding to file metadata access operations.
The format of Alluxio audit log entry is shown in the table below:

<table class="table table-striped">
<tr><th>key</th><th>value</th></tr>
<tr>
  <td>succeeded</td>
  <td>True if the command has succeeded. To succeed, it must also have been allowed. </td>
</tr>
<tr>
  <td>allowed</td>
  <td>True if the command has been allowed. Note that a command can still fail even if it has been allowed. </td>
</tr>
<tr>
  <td>ugi</td>
  <td>User group information, including username, primary group, and authentication type. </td>
</tr>
<tr>
  <td>ip</td>
  <td>Client IP address. </td>
</tr>
<tr>
  <td>cmd</td>
  <td>Command issued by the user. </td>
</tr>
<tr>
  <td>src</td>
  <td>Path of the source file or directory. </td>
</tr>
<tr>
  <td>dst</td>
  <td>Path of the destination file or directory. If not applicable, the value is null. </td>
</tr>
<tr>
  <td>perm</td>
  <td>User:group:mask or null if not applicable. </td>
</tr>
</table>

This is similar to the format of HDFS audit log [wiki](https://wiki.apache.org/hadoop/HowToConfigure).

To enable Alluxio audit logging, set the JVM property
`alluxio.master.audit.logging.enabled` to `true` in `alluxio-env.sh`.
See [Configuration settings]({{ '/en/basic/Configuration-Settings.html' | relativize_url }}).

## Encryption

<!-- ALLUXIO CS REPLACE -->
<!-- Service level encryption is not supported yet. -->
<!-- Users can encrypt sensitive data at the application level or enable encryption features in the  -->
<!-- respective under file system, such as HDFS transparent encryption or Linux disk encryption. -->
<!-- ALLUXIO CS WITH -->
Alluxio supports encryption of the network communication between services with TLS, and supports end-to-end data encryption.

### TLS Encryption for Network Communication
For Alluxio network communication (rpcs, data transfers), Alluxio supports TLS encryption. In order
to configure Alluxio to use TLS encryption, keystores and truststores must be created for Alluxio.
A keystore is used by the server side of the TLS connection, and the truststore is used by the
client side of the TLS connection.

#### Keystore
Alluxio servers (masters and workers) require a keystore in order to enable TLS. The keystore
typically stores the key and certificate for the server. This keystore file must be readable by the
OS user which launches the Alluxio server processes.

An example, self-signed keystore can be created like:

```bash
$ keytool -genkeypair -alias key -keyalg RSA -keysize 2048 -dname "cn=localhost, ou=Department, o=Company, l=City, st=State, c=US" -keystore /alluxio/keystore.jks -keypass keypass -storepass storepass
```

This will generate a keystore file to `/alluxio/keystore.jks`, with a key password of `keypass` and the keystore password as `storepass`.

#### Truststore
All clients of a TLS connection must have access to a truststore to trust all the certificates of
the servers. Clients include Alluxio clients, as well as Alluxio workers (since Alluxio workers
create client connections to the Alluxio master). The truststore stores the trusted certificates,
and must be readable by the process initiating the client connection (clients, workers).

An example truststore (based on the previous keystore) can be created like:

```bash
$ keytool -export -alias key -keystore /alluxio/keystore.jks -storepass storepass -rfc -file selfsigned.cer
$ keytool -import -alias key -noprompt -file selfsigned.cer -keystore /alluxio/truststore.jks -storepass trustpass
```

The first command extracts the certificate from the previously created keystore (using the keystore
password `storepass`). Then, the second command creates a truststore file using that extracted
certificate, and saves the truststore to `/alluxio/truststore.jks`, with a truststore pasword of
`trustpass`.

#### Configuring Alluxio servers and clients
Once the keystores and truststores are created for all the machines involved, Alluxio needs to be
configured to understand how to access those files.

On Alluxio servers (masters and workers), you must add these properties to
`alluxio-site.properties`:

```properties
# enables TLS
alluxio.network.tls.enabled=true
# keystore properties for the server side of connections
alluxio.network.tls.keystore.path=/alluxio/keystore.jks
alluxio.network.tls.keystore.password=storepass
alluxio.network.tls.keystore.key.password=keypass
# truststore properties for the client side of connections (worker to master)
alluxio.network.tls.truststore.path=/alluxio/truststore.jks
alluxio.network.tls.truststore.password=trustpass
```

Once the servers are configured, additional Alluxio clients need to be configured with the
client side properties:

```properties
# enables TLS
alluxio.network.tls.enabled=true
# truststore properties for the client side of connections (worker to master)
alluxio.network.tls.truststore.path=/alluxio/truststore.jks
alluxio.network.tls.truststore.password=trustpass
```

Setting these configuration properties will be dependent on the specific application or computation
framework you are using.

Once the servers and clients are configured, all network communication will be encrypted with TLS.

### End-to-End Data Encryption
Alluxio Enterprise Edition supports transparent end-to-end data encryption. Once configured, data will be
written to the Alluxio cluster encrypted and be read decrypted, without requiring
changes in application code. Data can only be encrypted and decrypted by the client, and the client
is responsible to get the crypto keys from an external Key Management Service (KMS).
Alluxio servers and admins are not able to access the crypto keys, thus can not make sense of the data
stored in Alluxio servers and under storage.

Alluxio end-to-end encryption achieves both data at-rest encryption and data in-flight encryption.
The end-to-end encryption is not applicable to Alluxio metadata.

Alluxio data encryption brings the following benefits:

1. Data Protection at Rest: malicious users can not make sense of the encrypted data residing on RAM/SSD/HDD.
2. Security on All Under Storages: applications can use various under storages via Alluxio,
with no worry about under storage encryption.
3. Secure Transmit over Network: eavesdroppers can not make sense of the encrypted data in flight.
4. Data Integrity: if encrypted data is manipulated, users can easily notice that it has been tampered with.

Here is an overview of a file write and read with encryption:

Alluxio is the single access point to all the encrypted data. Alluxio client is responsible for
encrypting the data, and writes the encrypted data to an Alluxio worker, which optionally (depending on the write type)
writes the encrypted data to UFS. When Alluxio client reads the data from an Alluxio worker,
it decrypts the data and then serves the decrypted data back to the application.

#### Key Management Service

Alluxio assumes there is an external Key Management Service (KMS) in the enterprise organization, and
integrates with it. Alluxio Enterprise Edition supports
[Hadoop KMS](https://hadoop.apache.org/docs/r2.8.0/hadoop-kms/index.html) out-of-box. Alluxio is
not responsible for encryption key maintenance and lifecycle management. Data is considered as deleted if
the secret key is deleted.

Please [contact](https://www.alluxio.com/) the Alluxio team for customized integration with other KMS types.

##### Hadoop KMS

An example configuration in `alluxio-site.properties` for using Hadoop KMS is below:

```properties
alluxio.security.kms.provider=HADOOP
alluxio.security.kms.endpoint=kms://http@localhost:16000/kms
```

If the Hadoop KMS is SSL enabled, import the kMS's certificate to your Java truststore by

```bash
$ openssl s_client -showcerts -connect host:port </dev/null 2>/dev/null | openssl x509 -outform PEM > /tmp/cert
$ keytool -import -noprompt -trustcacerts -alias localhost -file /tmp/cert -keystore ${JAVA_HOME}/jre/lib/security/cacerts
```

and set the `alluxio.security.kms.endpoint` to something like `kms://https@localhost:16000/kms`.

If the Hadoop KMS is Kerberos enabled, you need to specify the Kerberos principal and keytab file
for authenticating to the KMS by

```properties
alluxio.security.kms.kerberos.enabled=true
alluxio.security.kerberos.client.principal=<kms client principal>
alluxio.security.kerberos.client.keytab.file=<path to the kms client principal’s keytab file>
```

#### Cipher type and mode

By default, Alluxio Enterprise Edition uses the Advanced Encryption Standard (AES) algorithm
in Galois/Counter Mode (GCM), known as [AES-GCM](https://en.wikipedia.org/wiki/Galois/Counter_Mode).
Alluxio supports both 128-bit and 256-bit secret keys, which is determined by the secret key length provided
by the KMS. Alluxio uses symmetric encryption, where the same secret key is used to perform both
encryption and decryption.

AES-GCM is an authenticated encryption algorithm designed to provide both data authenticity (integrity)
and confidentiality. Authentication tags are produced during GCM encryption and must be supplied
during decryption. Alluxio stores the authentication tags within the ciphertext data and
records some encryption metadata (such as encryption id and encryption layout) in the footer
of the encrypted file. Therefore, the encrypted file size will be slightly larger than the plaintext
size. Alluxio clients are not aware of this space overhead because the logical plaintext sizes are
always shown to the Alluxio clients. Alluxio administrators will see slightly bigger files and blocks
on Alluxio server side.

#### Setup

By default, encryption is disabled in Alluxio Enterprise Edition.

To enable encryption in an Alluxio cluster, simply add the following configuration properties.

```
alluxio.security.encryption.enabled=true
alluxio.security.encryption.openssl.enabled=true
```

It is highly recommended to use the integration with [OpenSSL](https://www.openssl.org/) crypto library
for better encryption performance. The Alluxio Enterprise Edition comes with the pre-compiled JNI library
that connects with the OpenSSL library. [OpenSSL](https://www.openssl.org/) libcrypto is a
prerequisite to use Alluxio encryption with OpenSSL enabled. On unix servers, installing `openssl`
or `openssl-devel` will install the required OpenSSL packages. The Alluxio Enterprise Edition's pre-compiled JNI library
is compiled with OpenSSL 1.1. If you need to integrate with other versions of OpenSSL, please [contact us](mailto: support@alluxio.com).

Note that the pre-compiled JNI library is located at `${ALLUXIO_HOME}/lib/native/`. If you encountered
the following error indicating the required `.so` not found, please make sure the `${ALLUXIO_HOME}`
or `alluxio.home` is set properly.

```
java.lang.NoClassDefFoundError: Could not initialize class alluxio.client.security.OpenSSLCipher
    ......

Caused by: java.lang.UnsatisfiedLinkError: Can't load library: /opt/alluxio/lib/native/liballuxio.so
```

For testing purpose, Alluxio provides a dummy KMS which provides a fixed *testing-only* encryption key.
In order to connect Alluxio with a Key Management Service, please set the `alluxio.security.kms.provider`
and `alluxio.security.kms.endpoint`. Please refer to the example setup page for Alluxio with Hadoop KMS.

#### Encryption with UFS

Data is encrypted and decrypted in Alluxio clients, so only ciphertext will be persisted to the under
storage systems. If the under storage system supports encryption, it is recommended to disable UFS
encryption to avoid unnecessary double encryption. It is also recommended to setup an encrypted
Alluxio cluster without any pre-existing unencrypted files, because files in one Alluxio cluster
should be either all encrypted or all non-encrypted. In other words, the under storage system
is mounted to Alluxio as an empty UFS, and all UFS I/O happens through Alluxio.

Alluxio encryption supports re-mounting the UFS. Data encrypted by Alluxio and persisted in UFS
can be read and decrypted when UFS is unmounted and mounted back or across Alluxio restarts.
<!-- ALLUXIO CS END -->

## Deployment

It is required to start Alluxio master and workers using the same operating system user.
In the case where there is a user mismatch, secondary master healthcheck, the command `alluxio-start.sh all`
and certain file operations may fail because of permission checks.
