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
