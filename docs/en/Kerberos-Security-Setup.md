---
layout: global
title: Kerberos Security Setup
nickname: Kerberos
group: Features
priority: 1
---

* Table of Contents
{:toc}

This documentation describes how to set up an Alluxio cluster with
Kerberos security, running on a local machine as an example.

# Setup KDC

When setting up Kerberos, install the KDC first. If it is necessary to set up slave servers, 
install the master first. WARNING: It is best to install and run KDCs on 
secured and dedicated hardware with limited access.
If your KDC is also a file server, FTP server, Web server, or even just a client machine, 
someone who obtained root access through a security hole in any of those areas could potentially 
gain access to the Kerberos database.

Please follow this [guide](https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Managing_Smart_Cards/Configuring_a_Kerberos_5_Server.html)
to configure a KDC server on Linux.

Sample Alluxio KDC `krb5.conf`:

```bash
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
 kdc = ec2-54-208-45-116.compute-1.amazonaws.com
 admin_server = ec2-54-208-45-116.compute-1.amazonaws.com
}

[domain_realm]
 .alluxio.com = ALLUXIO.COM
 alluxio.com = ALLUXIO.COM
```
Note: after the KDC service is up, please make sure the firewall setting or Security Group on EC2
KDC machine is correctly set up with the following ports open:
(You can also disable some service ports as needed.)

```bash
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

# Configuring nodes with krb5 configs

Please set up a standalone KDC before doing this.

First, in the KDC server (not on the Alluxio nodes), do `sudo kadmin.local`
Create principles for Alluxio servers and clients:

```bash
addprinc -randkey alluxio/localhost@ALLUXIO.COM
addprinc -randkey client/localhost@ALLUXIO.COM
addprinc -randkey foo/localhost@ALLUXIO.COM
```

Second, in kadmin cli, create keytab files those principals:
```bash
xst -norandkey -k alluxio.keytab alluxio/localhost@ALLUXIO.COM
xst -norandkey -k client.keytab client/localhost@ALLUXIO.COM
xst -norandkey -k foo.keytab foo/localhost@ALLUXIO.COM
```
 
Thirdly, exist kadmin cli, set the correct permission for keytab files and
sanity check the keytab files.
```bash
sudo chmod 0644 alluxio.keytab
sudo chmod 0644 client.keytab
sudo chmod 0644 foo.keytab
```

Do `klist` and `kinit` to validate the keytab files are correctly generated.
```bash
klist -k -t -e alluxio.keytab
```
You should see a list of encrypted credentials for principal alluxio/localhost@ALLUXIO.COM
You can also do `kinit` to ensure the principal can be logged-in with those keytab files.
```bash
kinit -k -t alluxio.keytab alluxio/localhost@ALLUXIO.COM
```
Then `klist` should show the login user is alluxio/localhost@ALLUXIO.COM, with expiration date.
`kdestroy` will logout the current Kerberos user.

If you are unable to `kinit` or `klist` with the keytab files, please double check the commands
and principals, re-generate the keytab files until they passed those sanity checks. Invalid keytab
files are usually the reason for Kerberos authentication failures.

Fourthly, distribute the server and client keytab files to *each node* of the Alluxio cluster.
Save it in some secure place and configure the user and group permission coordinately.

```bash
scp -i ~/your_aws_key_pair.pem <KDC_DNS_NAME>:alluxio.keytab /etc/alluxio/conf/
scp -i ~/your_aws_key_pair.pem <KDC_DNS_NAME>:client.keytab /etc/alluxio/conf/
scp -i ~/your_aws_key_pair.pem <KDC_DNS_NAME>:foo.keytab /etc/alluxio/conf/
```

Create user alluxio and client.
```bash
adduser alluxio
adduser client
adduser foo
passwd hadoop
passwd client
passwd foo
```
mv the keytab files to some folder alluxio users can access:
```bash
sudo chown alluxio:alluxio /etc/alluxio/conf/alluxio.keytab
sudo chown client:alluxio /etc/alluxio/conf/client.keytab
sudo chown foo:alluxio /etc/alluxio/conf/foo.keytab
sudo chmod 0440 /etc/alluxio/conf/alluxio.keytab
sudo chmod 0440 /etc/alluxio/conf/client.keytab
sudo chmod 0440 /etc/alluxio/conf/foo.keytab
```

Finally, set up the client-side /etc/krb5.conf in each cluster node.

Sample:

```bash
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

[realms]
ALLUXIO.COM = {
 kdc = <KDC public IP address>
 admin_server = <KDC public IP address>
}

[domain_realm]
 .alluxio.com = ALLUXIO.COM
 alluxio.com = ALLUXIO.COM
```

# Server Configurations
There are several Alluxio configuration to set before starting a Kerberos-enabled cluster.

```
  alluxio.security.authentication.type=KERBEROS
  alluxio.security.authorization.permission.enabled=true
  alluxio.security.kerberos.server.principal=alluxio/localhost@ALLUXIO.COM
  alluxio.security.kerberos.server.keytab.file=/etc/alluxio/conf/alluxio.keytab
  alluxio.security.kerberos.client.principal=alluxio/localhost@ALLUXIO.COM
  alluxio.security.kerberos.client.keytab.file=/etc/alluxio/conf/alluxio.keytab
```

# Client Configuration
Client-side access to Alluxio cluster requires the following configurations:

```
  alluxio.security.authentication.type=KERBEROS
  alluxio.security.authorization.permission.enabled=true
  alluxio.security.kerberos.client.principal=client/localhost@ALLUXIO.COM
  alluxio.security.kerberos.client.keytab.file=/etc/alluxio/conf/client.keytab
```
  
You can switch users by changing the client principal and keytab pair.
Invalid combinations will get error message such as principal or keytab.file must be set.
The following error message shows that user can not be logged in via Kerberos.
```
Failed to login
```

# FAQ

## Receive timed out
Usually in a stack trace like
```
Caused by: java.net.SocketTimeoutException: Receive timed out
    at java.net.PlainDatagramSocketImpl.receive0(Native Method)
    at java.net.AbstractPlainDatagramSocketImpl.receive(AbstractPlainDatagramSocketImpl.java:146)
    at java.net.DatagramSocket.receive(DatagramSocket.java:816)
    at sun.security.krb5.internal.UDPClient.receive(NetClient.java:207)
    at sun.security.krb5.KdcComm$KdcCommunication.run(KdcComm.java:390)
    at sun.security.krb5.KdcComm$KdcCommunication.run(KdcComm.java:343)
```
 
This means the UDP socket awaiting a response from KDC eventually gave up.
Either the address of the KDC is wrong, or there's nothing at the far end listening for requests.

## Unable to obtain password from user
This is always because the keytab file is invalid, e.g. with wrong principle name,
or not set with right permission for alluxio:alluxio to access.

KDC log is your friend to see if any KDC requests are actually sent to KDC or not.

## javax.security.sasl.SaslException:GSS initiate failed 
javax.security.sasl.SaslException:GSS initiate failed: Caused by GSSException: No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt);

Solution: Check your JAVA_HOME to see if it’s pointing to a wrong version JDK, the java version matters here sometimes.

## No valid credentials provided (Mechanism level: Failed to find any Kerberos tgt

This error means the user is NOT authenticated.

Possible causes:
Your process was issued with a ticket, which has now expired.
You did specify a keytab but it isn't there or is somehow otherwise invalid
You don't have the Java Cryptography Extensions installed.
The principal isn't in the same realm as the service, so a matching TGT cannot be found. That is: you have a TGT, it's just for the wrong realm.
Your Active Directory tree has the same principal in more than one place in the tree.

## kinit -R failures

kinit: Ticket expired while renewing credentials

Solution 1: Check max_renewable_life in kdc.conf, set it to a positive value (say 10d) and restart KDC and kadmin services. Retry kinit -R

Solution 2: modprinc -maxrenewlife 10days krbtgt/<host>@<realm>

## Clock skew too great
This comes from the clocks on the machines being too far out of sync.
If it's a physical cluster, make sure that your NTP daemons are pointing at the same NTP server, one that is actually reachable from the Hadoop cluster.
And that the timezone settings of all the hosts are consistent.
