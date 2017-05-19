#!/usr/bin/env bash

alluxio_version=$(sed -n -e "s/^.*<version>\(.*\)<\/version>.*$/\1/p" < /alluxio/pom.xml | head -1)

grep "ALLUXIO_JAR" /alluxio/libexec/alluxio-config.sh | grep "alluxio-assembly"

if [[ "$?" == "0" ]]; then
  sed -i "s/alluxio-assembly-.*-jar-with-dependencies.jar/alluxio-assembly-${alluxio_version}-jar-with-dependencies.jar/g" /alluxio/libexec/alluxio-config.sh
else
  # Back compatibility
  sed -i "s/alluxio-.*-jar-with-dependencies.jar/alluxio-${alluxio_version}-jar-with-dependencies.jar/g" /alluxio/libexec/alluxio-config.sh
fi
