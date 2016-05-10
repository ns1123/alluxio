Alluxio Job Service
===================

## Running Alluxio Job Service

Similar to how the Alluxio system can be started and stopped using the `bin/alluxio-start.sh` and `bin/alluxio-stop.sh` scripts. The Alluxio job service can be started and stopped using `job/bin/alluxio-start.sh` and `job/bin/alluxio-stop.sh` scripts.

The Alluxio job service does not require a running Alluxio system to start, however, it might need require a running Alluxio system to perform any jobs that communicate with Alluxio.  

The Alluxio job service uses the same configuration mechanisms as the Alluxio system. Notably, the configuration is used to identify the Alluxio system with which the Alluxio job service should communicate. 
