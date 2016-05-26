/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.jdbc.csv;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

/**
 * Tests the methods of {@link CSVRowWriter}.
 */
public class CSVRowWriterTest {
  private CSVRowWriter mWriter;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() {
    mWriter = new CSVRowWriter();
  }

  @Test
  public void basicTest() throws IOException {
    mWriter.writeValue(new Integer(1));
    mWriter.writeValue("hello");
    mWriter.close();
    Assert.assertArrayEquals("1,hello\n".getBytes(), mWriter.getBytes());
  }

  @Test
  public void writeAfterCloseTest() throws IOException {
    mWriter.writeValue(new Integer(1));
    mWriter.close();
    mThrown.expect(IOException.class);
    mWriter.writeValue("hello");
  }

  @Test
  public void multipleCloseTest() throws IOException {
    mWriter.writeValue(new Integer(1));
    mWriter.writeValue("hello");
    mWriter.close();
    mWriter.close();
    mWriter.close();
    Assert.assertArrayEquals("1,hello\n".getBytes(), mWriter.getBytes());
  }

  @Test
  public void hasCommaTest() throws IOException {
    mWriter.writeValue(",");
    mWriter.writeValue(",");
    mWriter.close();
    // A string with a comma will enclose the string with double quotes
    Assert.assertArrayEquals("\",\",\",\"\n".getBytes(), mWriter.getBytes());
  }

  @Test
  public void hasCRTest() throws IOException {
    mWriter.writeValue("\r");
    mWriter.writeValue("\r");
    mWriter.close();
    // A string with a carriage return will enclose the string with double quotes
    Assert.assertArrayEquals("\"\r\",\"\r\"\n".getBytes(), mWriter.getBytes());
  }

  @Test
  public void hasLFTest() throws IOException {
    mWriter.writeValue("\n");
    mWriter.writeValue("\n");
    mWriter.close();
    // A string with a line feed will enclose the string with double quotes
    Assert.assertArrayEquals("\"\n\",\"\n\"\n".getBytes(), mWriter.getBytes());
  }

  @Test
  public void hasDoubleQuoteTest() throws IOException {
    mWriter.writeValue("\"");
    mWriter.writeValue("\"");
    mWriter.close();
    // A string with a double quote will enclose the string with double quotes. Also, the double
    // quote character will be escaped by preceeding it with another double quote character.
    Assert.assertArrayEquals("\"\"\"\",\"\"\"\"\n".getBytes(), mWriter.getBytes());
  }
}
