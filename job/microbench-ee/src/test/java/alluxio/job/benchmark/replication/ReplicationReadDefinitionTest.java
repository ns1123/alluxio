package alluxio.job.benchmark.replication;

import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.job.benchmark.IOThroughputResult;

import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by binfan on 3/6/17.
 */
public class ReplicationReadDefinitionTest extends TestCase {
  @Test
  void process() {
    ReplicationReadDefinition definition = new ReplicationReadDefinition();

    // Tell the read definition that it has read 1GB three times.
    ConcurrentLinkedQueue<Long> readBytesQueue = new ConcurrentLinkedQueue<Long>();
    readBytesQueue
        .addAll(Lists.newArrayList((long) Constants.GB, (long) Constants.GB, (long) Constants.GB));
    Whitebox.setInternalState(definition, "mReadBytesQueue", readBytesQueue);

    int threadNum = 3;
    ReplicationReadConfig config =
        new ReplicationReadConfig("64MB", "8MB", false, "128MB",
            "/fileToRead", ReadType.NO_CACHE.toString(),
            3, threadNum, false);
    List<List<Long>> timesNs = Lists.newArrayList();
    // Average time is 1 second, so average throughput is 1GB/s, or 1024MB/s.
    timesNs.add(Lists.newArrayList((long) 1e9, (long) 1.5e9, (long) 0.5e9));

    IOThroughputResult result = definition.process(config, timesNs);
    Assert.assertEquals(1024.0, result.getThroughput(), 0.00001);
    // 1e9 nanoseconds is 1000 milliseconds
    Assert.assertEquals(1000.0, result.getDuration(), 0.00001);
  }
}