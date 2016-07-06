```
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