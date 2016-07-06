```bash
./bin/alluxio fs ls /
./bin/alluxio fs mkdir /admin
./bin/alluxio fs mkdir /client
./bin/alluxio fs chown client /client
./bin/alluxio fs chgrp client /client
./bin/alluxio fs mkdir /foo
./bin/alluxio fs chown foo /foo
./bin/alluxio fs chgrp foo /foo
```
