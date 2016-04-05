```bash
scp -i ~/your_aws_key_pair.pem <KDC_DNS_NAME>:alluxio.keytab /etc/alluxio/conf/
scp -i ~/your_aws_key_pair.pem <KDC_DNS_NAME>:client.keytab /etc/alluxio/conf/
scp -i ~/your_aws_key_pair.pem <KDC_DNS_NAME>:foo.keytab /etc/alluxio/conf/
```