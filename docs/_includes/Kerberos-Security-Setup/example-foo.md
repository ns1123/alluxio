```bash
./bin/alluxio fs ls -R /
./bin/alluxio fs mkdir /foo/bar
./bin/alluxio fs copyFromLocal conf/alluxio-site.properties /foo/bar/testfile
./bin/alluxio fs copyFromLocal conf/alluxio-site.properties /client/foofile
```
