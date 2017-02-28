To build the Alluxio Docker image from the default url, run

```bash
docker build -t aee .
```

To build with a local Alluxio tarball, specify the `ALLUXIO_TARBALL` build argument

```bash
docker build -t aee --build-arg ALLUXIO_TARBALL=alluxio-enterprise-${version}.tar.gz .
```

The generated image expects to be run with single argument of "master" or "worker".
To set an Alluxio configuration property, convert it to an environment variable by uppercasing
and replacing periods with underscores. For example, `alluxio.master.hostname` converts to
`ALLUXIO_MASTER_HOSTNAME`. You can then set the environment variable on the image with
`-e PROPERTY=value`. Alluxio configuration values will be added to
`/opt/alluxio/conf/alluxio-site.properties` when the image starts.

```bash
docker run -d -e ALLUXIO_MASTER_HOSTNAME=ec2-203-0-113-25.compute-1.amazonaws.com aee [master|worker]
```

Additional configuration files can be included when building the image by adding them to the
`integration/docker-enterprise/conf/` directory. All contents of this directory will be
copied to `/opt/alluxio/conf`.

When running the master, an enterprise license is needed. This can be specified by passing
`-e ALLUXIO_LICENSE_BASE64=$(cat /path/to/license | base64)` to the `docker run aee master`
command.

```bash
docker run -d -e ALLUXIO_MASTER_HOSTNAME=ec2-203-0-113-25.compute-1.amazonaws.com -e ALLUXIO_LICENSE_BASE64=$(cat /path/to/license | base64) aee master
```
