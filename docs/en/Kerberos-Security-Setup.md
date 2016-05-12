---
layout: global
title: Kerberos Security Setup
nickname: Kerberos
group: Features
priority: 7
---

* Table of Contents
{:toc}

This documentation describes how to set up an Alluxio cluster with
[Kerberos](http://web.mit.edu/kerberos/) security, running on an AWS EC2 Linux machine locally as an example.

Some frequently seen problems and questions are listed at the end of the document.

# Setup KDC

When setting up Kerberos, install the [KDC](http://www.zeroshell.org/kerberos/Kerberos-definitions/#1.3.5) first. If it is necessary to set up KDC slave servers, 
install the KDC master first. WARNING: It is best to install and run KDCs on
secured and dedicated hardware with limited access.
If your KDC is also a file server, FTP server, Web server, or even just a client machine,
someone who obtained root access through a security hole in any of those areas could potentially
gain access to the Kerberos database.

Firstly, install all Kerberos required packages in this [page](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/installing-kerberos.html)

Then please follow this [guide](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/Configuring_a_Kerberos_5_Server.html)
to configure a KDC server on Linux.

Here is a sample Alluxio KDC `/etc/krb5.conf`:

{% include Kerberos-Security-Setup/kdc-krb5-conf.md %}

Edit `/var/kerberos/krb5kdc/kdc.conf` by replacing `EXAMPLE.COM` with `ALLUXIO.COM`.

Note: after the KDC service is up, please make sure the firewall settings 
(or Security Group on EC2 KDC machine) is correctly set up with the following ports open:
(You can also disable some service ports as needed.)

{% include Kerberos-Security-Setup/kdc-firewall-setting.md %}

# Setup kerberos client nodes

Please set up a standalone KDC before doing this.
Follow this [guide](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/Configuring_a_Kerberos_5_Client.html)
to set up the Kerberos client-side packages and configurations in each node in the Alluxio cluster (not the KDC node).
The Kerberos client settings also work if you want to setup local Alluxio cluster on Max OS X.

Alluxio cluster nodes `/etc/krb5.conf` sample:

{% include Kerberos-Security-Setup/client-krb5-conf.md %}

Verify the client-side Kerberos configurations by running `klist` and `kinit` commands.

# Generate keytab files in KDC

On the KDC node (not the Alluxio nodes), do `sudo kadmin.local CLI` to enter the kerberos admin console.

Firstly, create principles for Alluxio servers and clients:

{% include Kerberos-Security-Setup/kdc-add-principals.md %}

Secondly, create keytab files for those principals:

{% include Kerberos-Security-Setup/kdc-generate-keytab-files.md %}
 
Thirdly, exit the console by `CTRL + D`, and validate that the keytab files are correctly generated:

{% include Kerberos-Security-Setup/kdc-test-klist.md %}

You should see a list of encrypted credentials for principal `alluxio/localhost@ALLUXIO.COM`
You can also do `kinit` to ensure the principal can be logged-in with those keytab files:

{% include Kerberos-Security-Setup/kdc-test-kinit.md %}

Then `klist` should show the login user is `alluxio/localhost@ALLUXIO.COM`, with expiration date.
`kdestroy` will logout the current Kerberos user.

If you are unable to `kinit` or `klist` with the keytab files, please double check the commands
and principals, re-generate the keytab files until they pass the above sanity checks. Invalid keytab
files are usually the reason for Kerberos authentication failures.

# Setup Alluxio cluster with Kerberos security

Create user alluxio and clients.

{% include Kerberos-Security-Setup/add-users.md %}

Alluxio cluster will be running under User `alluxio`, so please add
`alluxio` to `sudoers` so that the user will have permission to ramdisks.

Add the following lines to the end of `/etc/sudoers`

{% include Kerberos-Security-Setup/add-sudoers.md %}

Login as user `alluxio` with

{% include Kerberos-Security-Setup/login-alluxio.md %}

All the following steps should be run as user `alluxio`.

Follow [Running-Alluxio-Locally](Running-Alluxio-Locally.html) or
[Running-Alluxio-on-a-Cluster](Running-Alluxio-on-a-cluster.html) to
install and start a Alluxio cluster without Kerberos security enabled.

Then, distribute the server and client keytab files from KDC to **each node** of the Alluxio cluster.
Save them in some secure place and configure the user and group permission coordinately, the following snippets save
the keytab files into `/etc/alluxio/conf`, create the directory on each Alluxio node if it does not exist.

{% include Kerberos-Security-Setup/distribute-keytab-files.md %}

{% include Kerberos-Security-Setup/set-keytab-files-permission.md %}

## Server Configuration
Put the following configurations into `conf/alluxio-site.properties`:

{% include Kerberos-Security-Setup/server-configs.md %}

Start the Alluxio cluster with:

{% include Kerberos-Security-Setup/start-alluxio.md %}

Verify that the cluster is running by `./bin/alluxio runTests` and access Web UI at port 19999.

## Client Configuration
Client-side access to Alluxio cluster requires the following configurations:
(Note: Server keytab file is not required for the client. The keytab files
permission are configured in a way that client users would not be able to access
server keytab file.)

{% include Kerberos-Security-Setup/client-configs.md %}

You can switch users by changing the client principal and keytab pair.
Invalid combinations will get error message such as principal or keytab.file must be set.
The following error message shows that user can not be logged in via Kerberos.

{% include Kerberos-Security-Setup/failed-to-login.md %}

Please see the FAQ section for more details about login failures.

# Example

You can play with the following examples to verify that the Alluxio cluster you setup is indeed
kerberos-enabled.

Firstly, act as super user `alluxio` by setting the following configurations in `conf/alluxio-site.properties`:

{% include Kerberos-Security-Setup/example-alluxio-configuration.md %}

Create some directories for different users via Alluxio filesystem shell:

{% include Kerberos-Security-Setup/example-alluxio.md %}

Now, you have `/admin` owned by user `alluxio`, `/client` owned by user `client`, and `/foo` owned by user `foo`.

If you change one or both of the above configurations to empty or a wrong value, then the kerberos authentication
should fail, so any command in `./bin/alluxio fs` should fail too.

Secondly, act as user `client` by re-configuring `conf/alluxio-site.properties`:

{% include Kerberos-Security-Setup/example-client-configuration.md %}

Create some directories and put some files into Alluxio:

{% include Kerberos-Security-Setup/example-client.md %}

The last two commands should fail since user `client` has no write permission to `/foo` which is owned by user `foo`.

Similarly, switch to user `foo` and try the filesystem shell:

{% include Kerberos-Security-Setup/example-foo-configuration.md %}

{% include Kerberos-Security-Setup/example-foo.md %}

The last command should fail because user `foo` has no write permission to `/client` which is owned by user `client`.

# FAQ

### Receive timed out
Usually in a stack trace like

{% include Kerberos-Security-Setup/receive-timeout-trace.md %}

This means the UDP socket awaiting a response from KDC eventually gave up.
Either the address of the KDC is wrong, or there's nothing at the far end listening for requests.

### Unable to obtain password from user
This is always because the keytab file is invalid, e.g. with wrong principle name,
or not set with right permission for alluxio:alluxio to access.

KDC log is your friend to tell whether KDC requests are actually sent to KDC.

### No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt)

This error means the user is NOT authenticated.

Possible causes:

- Your process was issued with a ticket, which has now expired.
- You did specify a keytab but it isn't there or is somehow otherwise invalid
- You don't have the Java Cryptography Extensions installed.
- The principal isn't in the same realm as the service, so a matching TGT cannot be found. That is: you have a TGT, it's just for the wrong realm.

### kinit -R failures

kinit: Ticket expired while renewing credentials

- Solution 1: Check `max_renewable_life` in kdc.conf, set it to a positive value (say 10d) and restart KDC and kadmin services. Retry `kinit -R`
- Solution 2
{% include Kerberos-Security-Setup/kinit-renew-solution.md %}

### Clock skew too great
This comes from the clocks on the machines being too far out of sync.
If it's a physical cluster, make sure that your NTP daemons are pointing at the same NTP server,
one that is actually reachable from the Hadoop cluster.
And that the timezone settings of all the hosts are consistent.
