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

package alluxio.underfs.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

/**
 * A class loader that uses an isolated URLClassLoader for classes with given prefixes from given
 * jars, otherwise fallbacks to the given class loader.
 */
public final class IsolatedClassLoader extends URLClassLoader {
  private static final Logger LOG = LoggerFactory.getLogger(IsolatedClassLoader.class);

  /**
   * A class loader to make the protected methods including findClass and loadClass in ClassLoader
   * accessible.
   */
  private static class DefaultClassLoader extends ClassLoader {
    /**
     * @param classLoader the parent class loader
     */
    public DefaultClassLoader(ClassLoader classLoader) {
      super(classLoader);
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
      return super.findClass(name);
    }

    @Override
    public  Class<?> loadClass(String name) throws ClassNotFoundException {
      return super.loadClass(name);
    }

    @Override
    public  Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      return super.loadClass(name, resolve);
    }
  }

  private final DefaultClassLoader mDefaultClassloader;
  private final List<String> mPrefixes;

  /**
   * @param jars array of URLs of jars
   * @param prefixes prefixes of class names that to use the isolated class loader
   * @param defaultClassLoader the default class loader to fall back
   */
  public IsolatedClassLoader(URL[] jars, String[] prefixes, ClassLoader defaultClassLoader) {
    super(jars, null);
    mDefaultClassloader = new DefaultClassLoader(defaultClassLoader);
    mPrefixes = Arrays.asList(prefixes);
    LOG.info("Classloader created, jars={}, prefixes={}", jars, prefixes);
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
    if (isPrefixMatching(name)) {
      return super.findClass(name);
    } else {
      return mDefaultClassloader.findClass(name);
    }

  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (isPrefixMatching(name)) {
      return super.loadClass(name, resolve);
    } else {
      return mDefaultClassloader.loadClass(name, resolve);
    }
  }

  /**
   * @param name name of the class
   * @return whether this class name matches any prefixes
   */
  private boolean isPrefixMatching(String name)  {
    for (String prefix : mPrefixes) {
      if (name.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}
