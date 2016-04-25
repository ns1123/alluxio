```bash
./bin/alluxio fs ls /
./bin/alluxio fs mkdir /admin
./bin/alluxio fs mkdir /client
./bin/alluxio fs chown client/localhost@ALLUXIO.COM /client
./bin/alluxio fs mkdir /foo
./bin/alluxio fs chown foo/localhost@ALLUXIO.COM /foo
```
