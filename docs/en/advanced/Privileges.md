---
layout: global
title: Privileges
nickname: Privileges
group: Advanced
---

* Table of Contents
{:toc}

This documentation describes how privileges work in the Alluxio system.

# Privilege model
Privileges are associated with groups. A user has a privilege if the user is a
member of a group which has been granted that privilege. See the
[authentication]({{ '/en/advanced/Security.html#authentication' | relativize_url }}) and
[group mapping]({{ '/en/advanced/Security.html#user-group-mapping' | relativize_url }}) sections of the security
documentation for details about setting up users and groups.

# Enabling privilege checking
Privileges are disabled by default - to enable them set 
`alluxio.security.privileges.enabled=true`. Privilege checking is done
on the master, so this property needs to be in the master configuration when the
master starts.

Privilege checking relies on being able to authenticate clients, so it is also
required that `alluxio.security.authentication.type` be set to something besides
`NOSASL`.

# Superuser privileges
Members of the supergroup (superusers) automatically have all privileges. The
supergroup is defined by the
`alluxio.security.authorization.permission.supergroup` property.

# Managing privileges
The `bin/alluxio privileges` shell command allows superusers to examine and
modify group privileges.

## Listing privileges
List privileges for all groups. This functionality is only available to
superusers.
```
$ bin/alluxio privileges list
```

List privileges for a specific group. Members of a group may list its privileges
even if they are not superusers.
```
$ bin/alluxio privileges list -group <group>
```

List privileges for a specific user. Users may list their own privileges even if
they are not superusers. This command will return the privileges granted to the
user individually, as well as any privileges they are granted through a group
they are a part of.
```
$ bin/alluxio privileges list -user <user>
```

## Granting privileges
Superusers can grant privileges with the `grant` subcommand.
```
$ bin/alluxio privileges grant -group <group> -privileges [privilege ...]
```

## Revoking privileges
Superusers can grant privileges with the `revoke` subcommand.
```
$ bin/alluxio privileges revoke -group <group> -privileges [privilege ...]
```

# List of Alluxio Privileges
## FREE
The FREE privilege allows users to free files from Alluxio memory. This can be
done either through the [CLI]({{ '/en/basic/Command-Line-Interface.html#free' | relativize_url }}) or through the
[Alluxio Filesystem API]({{ '/en/api/FS-API.html' | relativize_url }}).

## PIN
The PIN privilege allows users to pin or unpin files or directories in Alluxio
memory. This can be done either through the [CLI]({{ '/en/basic/Command-Line-Interface.html#pin' | relativize_url }})
or through the [Alluxio Filesystem API]({{ '/en/api/FS-API.html' | relativize_url }}).

## REPLICATION
The REPLICATION privilege allows users to modify file replication levels and
create new files with minimum replication set. This can be done either through
the [CLI]({{ '/en/basic/Command-Line-Interface.html#setreplication' | relativize_url }}) or through the
[Alluxio Filesystem API]({{ '/en/api/FS-API.html' | relativize_url }}).

## TTL
The TTL privilege allows users to modify the time to live (TTL) values of
files and directories. This can be done either through the
[CLI]({{ '/en/basic/Command-Line-Interface.html#setttl' | relativize_url }}) or through the
[Alluxio Filesystem API]({{ '/en/api/FS-API.html' | relativize_url }}).

# Troubleshooting privilege denied exceptions

You may encounter an exception complaining
```
User <user> does not have privilege <privilege>
```

Check what privileges the problematic user has.
```
$ bin/alluxio privileges list -user <user>
```

To have a privilege, the user must either be in the supergroup or be in a group
that has been granted the privilege.

Check the name of the Alluxio supergroup.
```
$ bin/alluxio getConf alluxio.security.authorization.permission.supergroup
```

Use the `groups` command to see what groups the user is in. This command should
be run from the Alluxio master machine in case the master has a different group
mapping from the client.
```
$ groups <user>
```

List the privileges for groups the user is in.
```
$ bin/alluxio privileges list -group <group>
```

To resolve the issue, either grant the missing privilege to one of the user's
groups, or [add the user]({{ '/en/advanced/Security.html#user-group-mapping' | relativize_url }}) to the Alluxio
supergroup.