package alluxio.master.journal.ufs;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.underfs.UnderFileSystemConfiguration;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link UfsJournal}.
 */
public class UfsJournalTest {
  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  @Test
  public void emptyConfiguration() throws Exception {
    UnderFileSystemConfiguration conf = UfsJournal.getJournalUfsConf();
    Assert.assertTrue(conf.getUserSpecifiedConf().isEmpty());
  }

  @Test
  public void nonEmptyConfiguration() throws Exception {
    String optionsPrefix = Configuration.get(PropertyKey.MASTER_JOURNAL_UFS_OPTION_PREFIX);
    PropertyKey key = PropertyKey.fromString(optionsPrefix + ".foo");
    String value = "bar";
    Configuration.set(key, value);
    UnderFileSystemConfiguration conf = UfsJournal.getJournalUfsConf();
    Assert.assertEquals(value, conf.getValue(key));
    Assert.assertEquals(1, conf.getUserSpecifiedConf().size());
  }
}
