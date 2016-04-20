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