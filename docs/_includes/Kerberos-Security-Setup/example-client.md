```bash
./bin/alluxio fs ls -R /
./bin/alluxio fs mkdir /client/dir
./bin/alluxio fs copyFromLocal conf/alluxio-site.properties /client/file
./bin/alluxio fs rm -R /client/dir
./bin/alluxio fs mkdir /foo/bar
./bin/alluxio fs rm -R /foo
```
