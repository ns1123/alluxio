```bash
sudo chown alluxio:alluxio /etc/alluxio/conf/alluxio.keytab
sudo chown client:alluxio /etc/alluxio/conf/client.keytab
sudo chown foo:alluxio /etc/alluxio/conf/foo.keytab

sudo chmod 0440 /etc/alluxio/conf/alluxio.keytab
sudo chmod 0440 /etc/alluxio/conf/client.keytab
sudo chmod 0440 /etc/alluxio/conf/foo.keytab
```